package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.exceptions.BlueDbException;

/**
 * A BlueCollectionBuilder is a builder for a BlueCollection.
 * @param <T> object type of values to be serialized into collection
 */
public interface BlueCollectionBuilder<T extends Serializable> {

	/**
	 * Registers classesToRegister with the serializer to make serialization and deserialization faster and more compact, then returns itself.
	 * @param classesToRegister classes that will be stored as values, in values, or in keys inside this collection.
	 * @return itself, after classesToRegister is added as classes to register
	 */
	public BlueCollectionBuilder<T> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister);

	/**
	 * Returns the existing BlueCollection or else builds a new one if none exists.
	 * @return existing BlueCollection<T> if one exists, else creates a new one
	 * @throws BlueDbException if any problems are encountered, such as collection already existing with a different type 
	 */
	public BlueCollection<T> build() throws BlueDbException;
}
