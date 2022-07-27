package org.bluedb.disk.query;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueQuery;
import org.bluedb.api.datastructures.BlueSimpleIterator;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.CollectionValueIterator;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.collection.index.conditions.AllSegmentsInRangeAcceptingIndexCondition;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.bluedb.disk.serialization.BlueEntity;

public class ReadOnlyQueryOnDisk<T extends Serializable> implements ReadBlueQuery<T> {

	protected ReadableCollectionOnDisk<T> collection;
	protected List<QueryIndexConditionGroup<T>> indexConditionGroups = new LinkedList<>();
	protected List<Condition<T>> objectConditions = new LinkedList<>();
	protected List<Condition<BlueKey>> keyConditions = new LinkedList<>();
	protected List<BlueSimpleSet<BlueKey>> keySetsToInclude = new LinkedList<>(); 
	protected long max = Long.MAX_VALUE;
	protected long min = Long.MIN_VALUE;
	protected boolean byStartTime = false;
	protected TimeIncludeMode timeIncludeMode = TimeIncludeMode.INCLUDE_ALL;

	public ReadOnlyQueryOnDisk(ReadableCollectionOnDisk<T> collection) {
		this.collection = collection;
	}

	@Override
	public ReadBlueQuery<T> where(Condition<T> c) {
		if (c != null) {
			objectConditions.add(c);
		}
		return this;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public ReadBlueQuery<T> where(BlueIndexCondition<?> indexCondition) {
		if(collection.isCompatibleIndexCondition(indexCondition)) {
			indexConditionGroups.add(new QueryIndexConditionGroup<T>(true, Arrays.asList((OnDiskIndexCondition<?, T>) indexCondition)));
		} else {
			throw new InvalidParameterException("The given indexCondition is invalid for this query. Queries and index conditions need to be created using the same collection in order to be compatible.");
		}
		return this;
	}
	
	@Override
	public ReadBlueQuery<T> whereKeyIsIn(BlueSimpleSet<BlueKey> keys) {
		if(keys != null) {
			keySetsToInclude.add(keys);
			keyConditions.add(key -> keys.contains(key));
		}
		return this;
	}

	@Override
	public List<T> getList() throws BlueDbException {
		return Blutils.map(getEntities(), (e) -> e.getValue());
	}
	
	@Override
	public Optional<T> getFirst() throws BlueDbException {
		try(CloseableIterator<T> queryIterator = getIterator()) {
			if(queryIterator.hasNext()) {
				return Optional.of(queryIterator.next());
			}
		}
		return Optional.empty();
	}

	@Override
	public CloseableIterator<T> getIterator() throws BlueDbException {
		finalizeParametersBeforeExecution();
		return new CollectionValueIterator<T>(collection.getSegmentManager(), getRange(), byStartTime, indexConditionGroups, objectConditions, keyConditions, getSegmentRangeInfoToInclude());
	}

	@Override
	public CloseableIterator<T> getIterator(long timeout, TimeUnit timeUnit) throws BlueDbException {
		long timeoutInMillis = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
		finalizeParametersBeforeExecution();
		return new CollectionValueIterator<T>(collection.getSegmentManager(), getRange(), timeoutInMillis, byStartTime, indexConditionGroups, objectConditions, keyConditions, getSegmentRangeInfoToInclude());
	}

	@Override
	public int count() throws BlueDbException {
		CloseableIterator<T> iter = getIterator();
		return iter.countRemainderAndClose();
	}
	
	public CloseableIterator<BlueEntity<T>> getEntityIterator() throws BlueDbException {
		finalizeParametersBeforeExecution();
		return new CollectionEntityIterator<T>(collection.getSegmentManager(), getRange(), byStartTime, indexConditionGroups, objectConditions, keyConditions, getSegmentRangeInfoToInclude());
	}

	public List<BlueEntity<T>> getEntities() throws BlueDbException {
		finalizeParametersBeforeExecution();
		return collection.findMatches(getRange(), indexConditionGroups, objectConditions, keyConditions, byStartTime, getSegmentRangeInfoToInclude());
	}

	private void finalizeParametersBeforeExecution() throws BlueDbException {
		if(collection.utilizesDefaultTimeIndex()) {

			addKeyConditionsForTimeIncludeMode();
			
			if(!byStartTime && min > Long.MIN_VALUE) {
				Range timeRange = getRange();
				SegmentPathManager pm = collection.getSegmentManager().getPathManager();
				
				long firstSegmentStartGroupingNumber = pm.getSegmentStartGroupingNumber(timeRange.getStart());
				long firstSegmentEndGroupingNumber = firstSegmentStartGroupingNumber + pm.getSegmentSize() - 1;
				
				indexConditionGroups.add(createDefaultTimeIndexConditionOrGroup(timeRange, firstSegmentStartGroupingNumber, firstSegmentEndGroupingNumber));
				
				//The indices will find the right segments to search, but not all keys in the segments will match the target range.
				keyConditions.add(key -> key.overlapsRange(timeRange.getStart(), timeRange.getEnd()));
				
				//Let the index conditions limit where we start searching from
				min = Long.MIN_VALUE;
			}
		}
	}

	private void addKeyConditionsForTimeIncludeMode() {
		if(timeIncludeMode == TimeIncludeMode.INCLUDE_ONLY_ACTIVE) {
			keyConditions.add(BlueKey::isActiveTimeKey);
		} else if(timeIncludeMode == TimeIncludeMode.EXCLUDE_ACTIVE) {
			keyConditions.add(key -> !key.isActiveTimeKey());
		}
	}

	@SuppressWarnings("unchecked")
	private QueryIndexConditionGroup<T> createDefaultTimeIndexConditionOrGroup(Range timeRange, long firstSegmentStartGroupingNumber, long firstSegmentEndGroupingNumber) throws UnsupportedIndexConditionTypeException, BlueDbException {
		QueryIndexConditionGroup<T> defaultTimeIndexConditionOrGroup = new QueryIndexConditionGroup<>(false);
		
		if(timeIncludeMode == TimeIncludeMode.INCLUDE_ALL) {
			//Find and include all records that start before the query timeframe and overlap into the timeframe
			defaultTimeIndexConditionOrGroup.addIndexCondition((OnDiskIndexCondition<?, T>) collection.getOverlappingTimeSegmentsIndex().createLongIndexCondition().isInRange(firstSegmentStartGroupingNumber, firstSegmentEndGroupingNumber));
			
			//Find and include all active records that start before the query timeframe
			defaultTimeIndexConditionOrGroup.addIndexCondition((OnDiskIndexCondition<?, T>) collection.getActiveRecordTimesIndex().createLongIndexCondition().isLessThan(firstSegmentStartGroupingNumber));
			
			//Include all segments that cover the query timeframe
			defaultTimeIndexConditionOrGroup.addIndexCondition(new AllSegmentsInRangeAcceptingIndexCondition<>(collection, timeRange));
		} else if(timeIncludeMode == TimeIncludeMode.EXCLUDE_ACTIVE) {
			//Find and include all records that start before the query timeframe and overlap into the timeframe
			defaultTimeIndexConditionOrGroup.addIndexCondition((OnDiskIndexCondition<?, T>) collection.getOverlappingTimeSegmentsIndex().createLongIndexCondition().isInRange(firstSegmentStartGroupingNumber, firstSegmentEndGroupingNumber));
			
			//Include all segments that cover the query timeframe
			defaultTimeIndexConditionOrGroup.addIndexCondition(new AllSegmentsInRangeAcceptingIndexCondition<>(collection, timeRange));
		} else if(timeIncludeMode == TimeIncludeMode.INCLUDE_ONLY_ACTIVE) {
			//Find and include all active records that start before or during the query timeframe
			defaultTimeIndexConditionOrGroup.addIndexCondition((OnDiskIndexCondition<?, T>) collection.getActiveRecordTimesIndex().createLongIndexCondition().isLessThanOrEqualTo(timeRange.getEnd()));
		}
		
		return defaultTimeIndexConditionOrGroup;
	}

	private Optional<IncludedSegmentRangeInfo> getSegmentRangeInfoToInclude() {
		IncludedSegmentRangeInfo segmentRangeInfoToInclude = new IncludedSegmentRangeInfo();
		
		ReadableSegmentManager<T> segmentManager = collection.getSegmentManager();
		SegmentPathManager pathManager = segmentManager.getPathManager();
		
		Range queryTimeframe = new Range(min, max);
		Range firstSegmentRangeInQueryTimeframe = segmentManager.toRange(pathManager.getSegmentPath(min));
		
		for(BlueSimpleSet<BlueKey> keysToInclude : keySetsToInclude) {
			try(BlueSimpleIterator<BlueKey> keysIterator = keysToInclude.iterator()) {
				while(keysIterator.hasNext()) {
					BlueKey key = keysIterator.next();
					if(key.overlapsRange(queryTimeframe.getStart(), queryTimeframe.getEnd())) {
						Range segmentRangeContainingKey = segmentManager.toRange(pathManager.getSegmentPath(key.getGroupingNumber()));
						if(!collection.utilizesDefaultTimeIndex() && segmentRangeContainingKey.getEnd() < firstSegmentRangeInQueryTimeframe.getStart()) {
							/*
							 * If this is a version 1 collection, the key overlaps the query timeframe but starts before
							 * the query timeframe, then the system will want to find that record in the pre-segment
							 * file of the first segment in the query timeframe. 
							 */
							segmentRangeInfoToInclude.addIncludedSegmentRangeInfo(firstSegmentRangeInQueryTimeframe, key.getGroupingNumber());
						} else {
							segmentRangeInfoToInclude.addIncludedSegmentRangeInfo(segmentRangeContainingKey, key.getGroupingNumber());
						}
					}
				}
			}
		}
		
		if(segmentRangeInfoToInclude.isEmpty()) {
			return Optional.empty();
		} else {
			return Optional.of(segmentRangeInfoToInclude);
		}
	}

	@Override
	public String toString() {
		return "<" + this.getClass().getSimpleName() + " [" + min + ", " + max + "] with " + objectConditions.size() + " conditions and " + keyConditions.size() + " key-conditions>";
	}

	private Range getRange() {
		return new Range(min, max);
	}

	protected ReadBlueQuery<T> afterTime(long time) {
		min = Math.max(min, Math.max(time + 1,time)); // last part to avoid overflow errors
		return this;
	}

	protected ReadBlueQuery<T> afterOrAtTime(long time) {
		min = Math.max(min, time);
		return this;
	}

	protected ReadBlueQuery<T> beforeTime(long time) {
		max = Math.min(max, Math.min(time - 1,time)); // last part to avoid overflow errors
		return this;
	}

	protected ReadBlueQuery<T> beforeOrAtTime(long time) {
		max = Math.min(max, time);
		return this;
	}

	protected ReadBlueQuery<T> byStartTime() {
		byStartTime = true;
		return this;
	}

	protected ReadBlueQuery<T> whereKeyIsActive() {
		if(timeIncludeMode == TimeIncludeMode.EXCLUDE_ACTIVE) {
			throw new IllegalStateException("You cannot call whereKeyIsActive if you have already called whereKeyIsNotActive");
		}
		
		timeIncludeMode = TimeIncludeMode.INCLUDE_ONLY_ACTIVE;
		return this;
	}

	protected ReadBlueQuery<T> whereKeyIsNotActive() {
		if(timeIncludeMode == TimeIncludeMode.INCLUDE_ONLY_ACTIVE) {
			throw new IllegalStateException("You cannot call whereKeyIsNotActive if you have already called whereKeyIsActive");
		}
		
		timeIncludeMode = TimeIncludeMode.EXCLUDE_ACTIVE;
		return this;
	}
	
	enum TimeIncludeMode {
		INCLUDE_ALL,
		INCLUDE_ONLY_ACTIVE,
		EXCLUDE_ACTIVE,
	}
}
