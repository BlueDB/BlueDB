package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

/**
 * A builder for the {@link BlueCollection} class
 * @param <V> object type of values to be serialized into the collection being built
 */
public interface BlueCollectionBuilder<K extends BlueKey, V extends Serializable> {

	/**
	 * Registers classesToRegister with the serializer to make serialization and deserialization faster and more compact, then returns itself.
	 * @param classesToRegister classes that will be stored as values, in values, or in keys inside this collection.
	 * @return itself, after classesToRegister is added as classes to register
	 */
	public BlueCollectionBuilder<K, V> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister);

	/**
	 * Requests a specific segment size to use for the collection. Values of the collection are grouped into segments and
	 * values in a segment can be vacuumed into a single file. Therefore, segment size will affect how fast you can access
	 * a specific value in a segment and how many i-nodes (files and directories) the collection will use on disk. This will 
	 * be ignored if the collection already exists. In the future BlueDB might support migrating from one segment size to another.
	 * 
	 * @param segmentSize the requested segment size for the collection. Values of the collection are grouped into segments and
	 * values in a segment can be vacuumed into a single file. Therefore, segment size will affect how fast you can access
	 * a specific value in a segment and how many i-nodes (files and directories) the collection will use on disk. This will be 
	 * ignored if the collection already exists. In the future BlueDB might support migrating from one segment size to another.
	 * 
	 * @return itself, after segmentSize is added as the requested segment size
	 * 
	 * @throws BlueDbException if the requested segment size is invalid
	 */
	public BlueCollectionBuilder<K, V> withSegmentSize(SegmentSize<K> segmentSize) throws BlueDbException;

	/**
	 * Sets the desired collection version to use if this is a new collection, then returns itself.
	 * @param version The blue collection version to use if this is a new collection. If not specified
	 * then {@link BlueCollectionVersion#getDefault()} will be used.
	 * @return itself, after version is set as the desired version.
	 */
	public BlueCollectionBuilder<K, V> withCollectionVersion(BlueCollectionVersion version);
	
	/**
	 * @return the existing {@link BlueCollection} or else builds a new one if none exists
	 * @throws BlueDbException if any problems are encountered, such as the collection already existing with a different type 
	 */
	public BlueCollection<V> build() throws BlueDbException;
	
}
