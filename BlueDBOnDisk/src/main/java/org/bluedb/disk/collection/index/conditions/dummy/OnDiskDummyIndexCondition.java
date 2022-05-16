package org.bluedb.disk.collection.index.conditions.dummy;

import java.io.Serializable;
import java.nio.file.Path;

import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.serialization.BlueEntity;

public interface OnDiskDummyIndexCondition<I extends Serializable, T extends Serializable> extends OnDiskIndexCondition<I, T> {

	public static final String DUMMY_INDEX_NAME = "DUMMY-INDEX";

	@Override
	public default String getIndexName() {
		return DUMMY_INDEX_NAME;
	}

	@Override
	public default Path getIndexPath() {
		return null;
	}

	@Override
	public default IncludedSegmentRangeInfo getSegmentRangeInfoToIncludeInCollectionQuery() {
		return new IncludedSegmentRangeInfo();
	}

	@Override
	public default boolean test(BlueEntity<T> entity) {
		return false;
	}

}
