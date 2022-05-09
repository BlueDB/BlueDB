package org.bluedb.api.index.conditions;

import java.security.InvalidParameterException;
import java.util.Set;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;

public interface IntegerIndexCondition extends BlueIndexCondition<Integer> {
	
	@Override
	public IntegerIndexCondition isEqualTo(Integer value);
	
	@Override
	public default IntegerIndexCondition isIn(Set<Integer> values) {
		if(values == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#isIn");
		}
		return isIn(new BlueSimpleInMemorySet<Integer>(values));
	}
	
	@Override
	public IntegerIndexCondition isIn(BlueSimpleSet<Integer> values);
	
	@Override
	public IntegerIndexCondition meets(Condition<Integer> condition);

	/**
	 * Only records with an indexed value within the given range will be included in the query.
	 * @param minValue - The min that an indexed value can be in order for the records that contain them to be
	 * included in the query.
	 * @param maxValue - The max that an indexed value can be in order for the records that contain them to be
	 * included in the query.
	 * @return
	 */
	public IntegerIndexCondition isInRange(int minValue, int maxValue);
	
	/**
	 * Only records with an indexed value less than the given value will be included in the query.
	 * @param value - The value that indexed values must less than in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public IntegerIndexCondition isLessThan(int value);
	
	/**
	 * Only records with an indexed value less than or equal to the given value will be included in the query.
	 * @param value - The value that indexed values must less than or equal to in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public IntegerIndexCondition isLessThanOrEqualTo(int value);
	
	/**
	 * Only records with an indexed value greater than the given value will be included in the query.
	 * @param value - The value that indexed values must greater than in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public IntegerIndexCondition isGreaterThan(int value);
	
	/**
	 * Only records with an indexed value greater than or equal to the given value will be included in the query.
	 * @param value - The value that indexed values must greater than or equal to in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public IntegerIndexCondition isGreaterThanOrEqualTo(int value);
}
