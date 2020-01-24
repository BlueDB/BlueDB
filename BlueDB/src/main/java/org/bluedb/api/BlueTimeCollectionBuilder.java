package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

/**
 * A builder for the {@link BlueTimeCollection} class
 * @param <V> object type of values to be serialized into the collection being built
 */
public interface BlueTimeCollectionBuilder<K extends BlueKey, V extends Serializable> extends BlueCollectionBuilder<K, V> {
	
	@Override
	BlueTimeCollectionBuilder<K, V> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister);

	@Override
	BlueTimeCollectionBuilder<K, V> withSegmentSize(SegmentSize<K> segmentSize) throws BlueDbException;

	/**
	 * @return the existing {@link BlueTimeCollection} or else builds a new one if none exists
	 * @throws BlueDbException if any problems are encountered, such as the collection already existing with a different type 
	 */
	public BlueTimeCollection<V> build() throws BlueDbException;
}
