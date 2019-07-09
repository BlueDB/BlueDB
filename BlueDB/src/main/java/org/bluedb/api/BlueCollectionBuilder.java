package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

public interface BlueCollectionBuilder<K extends BlueKey, V extends Serializable> {
	public BlueCollectionBuilder<K, V> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister);
	public BlueCollectionBuilder<K, V> withSegmentSize(SegmentSize<K> segmentSize) throws BlueDbException;
	public BlueCollection<V> build() throws BlueDbException;
}
