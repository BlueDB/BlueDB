package org.bluedb.api.index.conditions;

import java.security.InvalidParameterException;
import java.util.Set;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;

public interface StringIndexCondition extends BlueIndexCondition<String> {
	
	@Override
	public StringIndexCondition isEqualTo(String value);
	
	@Override
	public default StringIndexCondition isIn(Set<String> values) {
		if(values == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#isIn");
		}
		return isIn(new BlueSimpleInMemorySet<String>(values));
	}
	
	@Override
	public StringIndexCondition isIn(BlueSimpleSet<String> values);
	
	@Override
	public StringIndexCondition meets(Condition<String> condition);
	
}
