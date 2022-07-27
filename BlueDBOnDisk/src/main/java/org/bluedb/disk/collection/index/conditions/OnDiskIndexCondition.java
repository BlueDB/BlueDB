package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;
import java.nio.file.Path;

import org.bluedb.api.index.conditions.BlueIndexCondition;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.serialization.BlueEntity;

public interface OnDiskIndexCondition<I extends Serializable, T extends Serializable> extends BlueIndexCondition<I> {
	
	public Class<T> getIndexedCollectionType();
	
	public Class<? extends ValueKey> getIndexKeyType();
	
	public String getIndexName();
	
	public Path getIndexPath(); 
	
	public IncludedSegmentRangeInfo getSegmentRangeInfoToIncludeInCollectionQuery();
	
	public boolean test(BlueEntity<T> entity);
	
}
