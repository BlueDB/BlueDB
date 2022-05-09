package org.bluedb.disk.query;

import java.io.Serializable;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.ReadBlueQuery;
import org.bluedb.api.datastructures.BlueSimpleIterator;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.CollectionValueIterator;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.collection.index.conditions.AllSegmentsInRangeAcceptingIndexCondition;
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
	public CloseableIterator<T> getIterator() throws BlueDbException {
		finalizeParametersBeforeExecution();
		return new CollectionValueIterator<T>(collection.getSegmentManager(), getRange(), byStartTime, indexConditionGroups, objectConditions, keyConditions, getSegmentRangesToInclude());
	}

	@Override
	public CloseableIterator<T> getIterator(long timeout, TimeUnit timeUnit) throws BlueDbException {
		long timeoutInMillis = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
		finalizeParametersBeforeExecution();
		return new CollectionValueIterator<T>(collection.getSegmentManager(), getRange(), timeoutInMillis, byStartTime, indexConditionGroups, objectConditions, keyConditions, getSegmentRangesToInclude());
	}

	@Override
	public int count() throws BlueDbException {
		CloseableIterator<T> iter = getIterator();
		return iter.countRemainderAndClose();
	}
	
	public CloseableIterator<BlueEntity<T>> getEntityIterator() throws BlueDbException {
		finalizeParametersBeforeExecution();
		return new CollectionEntityIterator<T>(collection.getSegmentManager(), getRange(), byStartTime, indexConditionGroups, objectConditions, keyConditions, getSegmentRangesToInclude());
	}

	public List<BlueEntity<T>> getEntities() throws BlueDbException {
		finalizeParametersBeforeExecution();
		return collection.findMatches(getRange(), indexConditionGroups, objectConditions, keyConditions, byStartTime, getSegmentRangesToInclude());
	}

	@SuppressWarnings("unchecked")
	private void finalizeParametersBeforeExecution() throws BlueDbException {
		if(!collection.utilizesDefaultTimeIndex() || byStartTime || min == Long.MIN_VALUE) {
			/*
			 * If it doesn't utilize the default time index then we have to fallback on the default behavior.
			 * If it is by start time or already looking from the dawn of time then we don't need to worry about 
			 * finding overlapping records from the past.
			 */
			return;
		}
		
		Range timeRange = getRange();
		SegmentPathManager pm = collection.getSegmentManager().getPathManager();
		
		long startSegmentGroupingNumber = pm.getSegmentStartGroupingNumber(timeRange.getStart());
		
		QueryIndexConditionGroup<T> defaultTimeIndexConditionOrGroup = new QueryIndexConditionGroup<>(false);
		
		/*
		 * Utilize the time segments index to find all records that overlap the start segment. This will find
		 * all records that started before the start segment and ended during or after the start segment.
		 */
		defaultTimeIndexConditionOrGroup.addIndexCondition((OnDiskIndexCondition<?, T>) collection.getOverlappingTimeSegmentsIndex().createLongIndexCondition().isEqualTo(startSegmentGroupingNumber));
		
		/*
		 * Or with the all segments in range accepting index condition since it doesn't really save time trying to 
		 * narrow down what segments inside the range to search. We already know that everything stored in the
		 * timeframe started in the timeframe.
		 */
		defaultTimeIndexConditionOrGroup.addIndexCondition(new AllSegmentsInRangeAcceptingIndexCondition<>(collection, timeRange));
		
		indexConditionGroups.add(defaultTimeIndexConditionOrGroup);
		
		//The indices will find the right segments to search, but not all keys in the segments will match the target range.
		keyConditions.add(key -> key.isInRange(timeRange.getStart(), timeRange.getEnd()));
		
		//Let the index conditions limit where we start searching from
		min = Long.MIN_VALUE;
	}

	private Optional<Set<Range>> getSegmentRangesToInclude() {
		Set<Range> segmentRangesToInclude = new HashSet<>();
		
		ReadableSegmentManager<T> segmentManager = collection.getSegmentManager();
		SegmentPathManager pathManager = segmentManager.getPathManager();
		
		for(BlueSimpleSet<BlueKey> keysToInclude : keySetsToInclude) {
			try(BlueSimpleIterator<BlueKey> keysIterator = keysToInclude.iterator()) {
				while(keysIterator.hasNext()) {
					BlueKey key = keysIterator.next();
					Path segmentPath = pathManager.getSegmentPath(key);
					segmentRangesToInclude.add(segmentManager.toRange(segmentPath));
				}
			}
		}
		
		if(segmentRangesToInclude.isEmpty()) {
			return Optional.empty();
		} else {
			return Optional.of(segmentRangesToInclude);
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
}
