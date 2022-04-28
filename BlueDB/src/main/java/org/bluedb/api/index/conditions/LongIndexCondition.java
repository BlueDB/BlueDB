package org.bluedb.api.index.conditions;

import java.security.InvalidParameterException;
import java.util.Set;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;

public interface LongIndexCondition extends BlueIndexCondition<Long> {
	
	@Override
	public LongIndexCondition isEqualTo(Long value);
	
	@Override
	public default LongIndexCondition isIn(Set<Long> values) {
		if(values == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#isIn");
		}
		return isIn(new BlueSimpleInMemorySet<Long>(values));
	}
	
	@Override
	public LongIndexCondition isIn(BlueSimpleSet<Long> values);
	
	@Override
	public LongIndexCondition meets(Condition<Long> condition);
	
	/**
	 * Only records with an indexed value less than the given value will be included in the query.
	 * @param value - The value that indexed values must less than in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public LongIndexCondition isLessThan(long value);
	
	/**
	 * Only records with an indexed value less than or equal to the given value will be included in the query.
	 * @param value - The value that indexed values must less than or equal to in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public LongIndexCondition isLessThanOrEqualTo(long value);
	
	/**
	 * Only records with an indexed value greater than the given value will be included in the query.
	 * @param value - The value that indexed values must greater than in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public LongIndexCondition isGreaterThan(long value);
	
	/**
	 * Only records with an indexed value greater than or equal to the given value will be included in the query.
	 * @param value - The value that indexed values must greater than or equal to in order for the records that contain them to be 
	 * included in a query.
	 * @return itself with the condition added.
	 */
	public LongIndexCondition isGreaterThanOrEqualTo(long value);
}
