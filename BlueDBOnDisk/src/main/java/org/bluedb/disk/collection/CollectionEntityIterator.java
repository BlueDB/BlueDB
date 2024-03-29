package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.Condition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.query.QueryIndexConditionGroup;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegment;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.SegmentEntityIterator;
import org.bluedb.disk.serialization.BlueEntity;

public class CollectionEntityIterator<T extends Serializable> implements CloseableIterator<BlueEntity<T>> {

	final private List<? extends ReadableSegment<T>> segments;
	final private Range range;
	private long endGroupingValueOfCompletedSegments;
	private SegmentEntityIterator<T> segmentIterator;
	private BlueEntity<T> next;
	private final List<QueryIndexConditionGroup<T>> indexConditionGroups;
	private final List<Condition<T>> conditions;
	private final List<Condition<BlueKey>> keyConditions;
	private final Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfoAfterApplyingIndexConditions;
	
	private AtomicBoolean hasClosed = new AtomicBoolean(false);

	public CollectionEntityIterator(final ReadableSegmentManager<T> segmentManager, Range range, boolean byStartTime, List<QueryIndexConditionGroup<T>> indexConditionGroups, List<Condition<T>> objectConditions, List<Condition<BlueKey>> keyConditions, Optional<IncludedSegmentRangeInfo> includedIndexSegmentRangeInfo) {
		this.range = range;
		this.endGroupingValueOfCompletedSegments = calculateEndGroupingValueOfCompletedSegments(range, byStartTime);
		this.indexConditionGroups = indexConditionGroups;
		this.includedSegmentRangeInfoAfterApplyingIndexConditions = getIncludedSegmentRangeInfoAfterApplyingIndexConditions(includedIndexSegmentRangeInfo);
		this.segments = segmentManager.getExistingSegments(range, includedSegmentRangeInfoAfterApplyingIndexConditions);
		Collections.sort(segments);
		this.conditions = objectConditions;
		this.keyConditions = keyConditions;
	}

	private long calculateEndGroupingValueOfCompletedSegments(Range range, boolean byStartTime) {
		if(!byStartTime || range.getStart() == Long.MIN_VALUE) {
			return Long.MIN_VALUE;
		}
		
		return range.getStart() - 1;
	}

	private Optional<IncludedSegmentRangeInfo> getIncludedSegmentRangeInfoAfterApplyingIndexConditions(Optional<IncludedSegmentRangeInfo> originalIncludedIndexSegmentRangeInfo) {
		if(!StreamUtils.stream(indexConditionGroups)
			.flatMap(indexConditionGroup -> StreamUtils.stream(indexConditionGroup.getIndexConditions()))
			.findAny()
			.isPresent()) {
			return originalIncludedIndexSegmentRangeInfo;
		}
		
		IncludedSegmentRangeInfo includedSegmentRangeInfoAfterIndexConditions = null;
		for(QueryIndexConditionGroup<T> indexConditionGroup : indexConditionGroups) {
			IncludedSegmentRangeInfo includedSegmentRangeInfoForConditionGroup = null;
			for(OnDiskIndexCondition<?, T> indexCondition : indexConditionGroup.getIndexConditions()) {
				IncludedSegmentRangeInfo includedSegmentRangeInfoForCondition = indexCondition.getSegmentRangeInfoToIncludeInCollectionQuery();
				if(includedSegmentRangeInfoForConditionGroup == null) {
					includedSegmentRangeInfoForConditionGroup = includedSegmentRangeInfoForCondition != null ? new IncludedSegmentRangeInfo(includedSegmentRangeInfoForCondition) : null;
				} else {
					includedSegmentRangeInfoForConditionGroup.combine(includedSegmentRangeInfoForCondition, indexConditionGroup.isShouldAnd());
				}
			}
			
			if(includedSegmentRangeInfoAfterIndexConditions == null) {
				includedSegmentRangeInfoAfterIndexConditions = includedSegmentRangeInfoForConditionGroup != null ? includedSegmentRangeInfoForConditionGroup : null;
			} else {
				includedSegmentRangeInfoAfterIndexConditions.combine(includedSegmentRangeInfoForConditionGroup, true);
			}
		}
		
		if(includedSegmentRangeInfoAfterIndexConditions == null) {
			return originalIncludedIndexSegmentRangeInfo;
		}
		
		if(originalIncludedIndexSegmentRangeInfo.isPresent()) {
			includedSegmentRangeInfoAfterIndexConditions.combine(originalIncludedIndexSegmentRangeInfo.get(), true);
		}
		
		return Optional.of(includedSegmentRangeInfoAfterIndexConditions);
	}

	@Override
	public synchronized void close() {
		if(!hasClosed.getAndSet(true) && segmentIterator != null) {
			segmentIterator.close();
		}
	}

	@Override
	public synchronized boolean hasNext() {
		if (hasClosed.get()) {
			throw new RuntimeException("CollectionEntityIterator has already been closed");
		}
		
		if (next == null) {
			next = nextFromSegment();
		}
		return next != null;
	}

	@Override
	public synchronized BlueEntity<T> peek() {
		if (hasClosed.get()) {
			throw new RuntimeException("CollectionEntityIterator has already been closed");
		}
		
		if (next == null) {
			next = nextFromSegment();
		}
		return next;
	}

	@Override
	public synchronized BlueEntity<T> next() {
		if (hasClosed.get()) {
			throw new RuntimeException("CollectionEntityIterator has already been closed");
		}
		
		if (next == null) {
			next = nextFromSegment();
		}
		BlueEntity<T> response = next;
		next = null;
		return response;
	}

	private BlueEntity<T> nextFromSegment() {
		while (!segments.isEmpty() || segmentIterator != null) {
			if (segmentIterator != null) {
				while(segmentIterator.hasNext()) {
					BlueEntity<T> result = segmentIterator.next();
					if (Blutils.meetsConditions(conditions, result.getValue()) &&
							Blutils.meetsConditions(keyConditions, result.getKey()) &&
							Blutils.meetsIndexConditions(indexConditionGroups, result)) {
						return result;
					}
				}
			}
			if (segmentIterator != null) {
				segmentIterator.close();
			}
			segmentIterator = getNextSegmentIterator();
		}
		return null;
	}

	private SegmentEntityIterator<T> getNextSegmentIterator() {
		if (segments.isEmpty()) {
			return null;
		}
		if (segmentIterator != null) {
			long endOfLastSegment =  segmentIterator.getSegment().getRange().getEnd();
			endGroupingValueOfCompletedSegments = endOfLastSegment;
		}
		ReadableSegment<T> segment = segments.remove(0);
		Optional<Range> segmentRangeToSearchAccordingToIncludedSegmentInfo = includedSegmentRangeInfoAfterApplyingIndexConditions.map(includedSegmentRangeInfo -> includedSegmentRangeInfo.getRangeForSegment(segment.getRange()));
		Range segmentRangeToSearch = segmentRangeToSearchAccordingToIncludedSegmentInfo.orElse(range);
		return segment.getIterator(endGroupingValueOfCompletedSegments, segmentRangeToSearch, segmentRangeToSearchAccordingToIncludedSegmentInfo.isPresent());
	}

	@Override
	public void keepAlive() {
		
	}
}
