package org.bluedb.api.index;

import java.io.Serializable;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.conditions.IntegerIndexCondition;
import org.bluedb.api.index.conditions.LongIndexCondition;
import org.bluedb.api.index.conditions.StringIndexCondition;
import org.bluedb.api.index.conditions.UUIDIndexCondition;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;

/**
 * An index on a {@link BlueCollection} that allows you to query for values faster based on specific data in those values.
 * BlueDB extracts data from each value in the collection and creates a mapping from that data to the values that contain that
 * data. Currently this only supports the retrieval of values using a specific index key, but could be extended in the future to
 * provide more powerful index based queries.
 * 
 * @param <K> the key type of the index or the type of data that the collection is being indexed on. It must be a concretion of 
 * {@link ValueKey} ({@link UUIDKey}, {@link StringKey}, {@link LongKey}, or {@link IntegerKey}).
 * @param <V> the value type of the collection being indexed
 */
public interface BlueIndex<K extends ValueKey, V extends Serializable> {

	/**
	 * @return the index key with the highest grouping number
	 */
	public K getLastKey();
	
	/**
	 * Creates an integer index condition that can be modified and added to the where clause of a collection
	 * query. Using a condition on indexed information can greatly increase the efficiency of a query by
	 * allowing BlueDB to more quickly identify what segments it needs to load the full record data from.
	 * @return an integer index condition that can be modified and added to a collection query's where
	 * clause. 
	 * @throws UnsupportedIndexConditionTypeException if this index is not the correct type for this index condition
	 */
	public IntegerIndexCondition createIntegerIndexCondition() throws UnsupportedIndexConditionTypeException;
	
	/**
	 * Creates a long index condition that can be modified and added to the where clause of a collection
	 * query. Using a condition on indexed information can greatly increase the efficiency of a query by
	 * allowing BlueDB to more quickly identify what segments it needs to load the full record data from.
	 * @return an integer index condition that can be modified and added to a collection query's where
	 * clause. 
	 * @throws UnsupportedIndexConditionTypeException if this index is not the correct type for this index condition
	 */
	public LongIndexCondition createLongIndexCondition() throws UnsupportedIndexConditionTypeException;
	
	/**
	 * Creates a string index condition that can be modified and added to the where clause of a collection
	 * query. Using a condition on indexed information can greatly increase the efficiency of a query by
	 * allowing BlueDB to more quickly identify what segments it needs to load the full record data from.
	 * @return an integer index condition that can be modified and added to a collection query's where
	 * clause. 
	 * @throws UnsupportedIndexConditionTypeException if this index is not the correct type for this index condition
	 */
	public StringIndexCondition createStringIndexCondition() throws UnsupportedIndexConditionTypeException;
	
	/**
	 * Creates a UUID index condition that can be modified and added to the where clause of a collection
	 * query. Using a condition on indexed information can greatly increase the efficiency of a query by
	 * allowing BlueDB to more quickly identify what segments it needs to load the full record data from.
	 * @return an integer index condition that can be modified and added to a collection query's where
	 * clause. 
	 * @throws UnsupportedIndexConditionTypeException if this index is not the correct type for this index condition
	 */
	public UUIDIndexCondition createUUIDIndexCondition() throws UnsupportedIndexConditionTypeException;
}
