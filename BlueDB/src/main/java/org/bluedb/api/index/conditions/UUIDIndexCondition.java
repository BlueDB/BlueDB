package org.bluedb.api.index.conditions;

import java.security.InvalidParameterException;
import java.util.Set;
import java.util.UUID;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleInMemorySet;
import org.bluedb.api.datastructures.BlueSimpleSet;

public interface UUIDIndexCondition extends BlueIndexCondition<UUID> {
	
	@Override
	public UUIDIndexCondition isEqualTo(UUID value);
	
	@Override
	public default UUIDIndexCondition isIn(Set<UUID> values) {
		if(values == null) {
			throw new InvalidParameterException("Null is an invalid parameter for BlueIndexCondition#isIn");
		}
		return isIn(new BlueSimpleInMemorySet<UUID>(values));
	}
	
	@Override
	public UUIDIndexCondition isIn(BlueSimpleSet<UUID> values);
	
	@Override
	public UUIDIndexCondition meets(Condition<UUID> condition);
	
}
