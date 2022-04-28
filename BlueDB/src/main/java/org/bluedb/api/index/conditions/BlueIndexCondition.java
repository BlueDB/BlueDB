package org.bluedb.api.index.conditions;

import java.io.Serializable;
import java.util.Set;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;

public interface BlueIndexCondition<V extends Serializable> {
	
	/**
	 * Only records with the given indexed value will be included in the query.
	 * @param value - The value that indexed values must equal in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public BlueIndexCondition<V> isEqualTo(V value);
	
	/**
	 * Only records with an indexed value contained in the given set will be included in the query.
	 * @param values - Index values must be contained in this set in order for records containing 
	 * them to be included in this query.
	 * @return itself with the condition added.
	 */
	public BlueIndexCondition<V> isIn(Set<V> values);
	
	/**
	 * Only records with an indexed value contained in the given set will be included in the query. This option allows you
	 * to specify your own set implementation which doesn't have to have everything in memory at the same time.
	 * @param values - Index values must be contained in this set in order for records containing 
	 * them to be included in this query.
	 * @return itself with the condition added.
	 */
	public BlueIndexCondition<V> isIn(BlueSimpleSet<V> values);
	
	/**
	 * Allows you to specify a condition that indexed values must meet in order for the records that contain them to be 
	 * included in a query. Note that this condition must be blindly applied to all indexed values so other methods are 
	 * more efficient.
	 * @param condition - The condition that indexed values must meet in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public BlueIndexCondition<V> meets(Condition<V> condition);
	
}
