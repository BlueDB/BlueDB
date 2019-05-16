package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

public interface BlueCollectionBuilder<K extends BlueKey, T extends Serializable> {
	public BlueCollectionBuilder<K, T> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister);
	public BlueCollectionBuilder<K, T> withRequestedSegmentSize(SegmentSize<K> segmentSize);
	public BlueCollection<T> build() throws BlueDbException;
}
