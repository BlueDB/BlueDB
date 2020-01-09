package org.bluedb.api;

import java.io.Serializable;

import org.bluedb.api.keys.TimeKey;

/**
 * A ReadOnlyBlueTimeCollection represents a persisted map of keys (of type {@link TimeKey}) to values of object type V.
 * 
 * A collection has a name (to distinguish between collections in a {@link BlueDb} instance), a key type, and a value type.
 * @param <V> the object type of values to be serialized into the collection
 */
public interface ReadOnlyBlueTimeCollection<V extends Serializable> extends ReadOnlyBlueCollection<V> {

	/**
	 * Creates a {@link ReadOnlyBlueTimeQuery} object which can be used to build and execute a query against this collection.
	 * @return a {@link ReadOnlyBlueTimeQuery} object which can be used to build and execute a query against this collection.
	 */
	@Override
	public ReadOnlyBlueTimeQuery<V> query();

}
