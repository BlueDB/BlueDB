package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.collection.ReadableCollectionOnDisk;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.serialization.BlueEntity;

public class AllSegmentsInRangeAcceptingIndexCondition<I extends Serializable, T extends Serializable> extends OnDiskIndexConditionBase<I, T> {
	private final ReadableSegmentManager<T> collectionSegmentManager;
	private final Range groupingNumberRangeToAccept;
	
	public AllSegmentsInRangeAcceptingIndexCondition(ReadableCollectionOnDisk<T> collection, Range groupingNumberRangeToAccept) throws BlueDbException {
		super((ReadableIndexOnDisk<LongTimeKey, T>) collection.getOverlappingTimeSegmentsIndex());
		this.collectionSegmentManager = collection.getSegmentManager();
		this.groupingNumberRangeToAccept = groupingNumberRangeToAccept;
	}
	
	@Override
	public Set<Range> getSegmentRangesToIncludeInCollectionQuery() {
		return StreamUtils.stream(collectionSegmentManager.getExistingSegmentRanges(groupingNumberRangeToAccept, Optional.empty()))
				.collect(Collectors.toCollection(HashSet::new));
	}
	
	@Override
	public boolean test(BlueEntity<T> entity) {
		return entity.getKey().isInRange(groupingNumberRangeToAccept.getStart(), groupingNumberRangeToAccept.getEnd());
	}

	@Override
	public BlueIndexCondition<I> isIn(Set<I> values) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected I extractIndexValueFromKey(BlueKey indexKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected ValueKey createKeyForIndexValue(I value) {
		throw new UnsupportedOperationException();
	}

}
