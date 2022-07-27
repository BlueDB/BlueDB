package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.StringIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;

public class OnDiskStringIndexCondition<T extends Serializable> extends OnDiskIndexConditionBase<String, T> implements StringIndexCondition {
	
	public OnDiskStringIndexCondition(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		super(index);
	}

	@Override
	public StringIndexCondition isEqualTo(String value) {
		super.isEqualTo(value);
		return this;
	}
	
	@Override
	public StringIndexCondition isIn(BlueSimpleSet<String> values) {
		super.isIn(values);
		return this;
	}
	
	@Override
	public StringIndexCondition meets(Condition<String> condition) {
		super.meets(condition);
		return this;
	}

	@Override
	protected ValueKey createKeyForIndexValue(String value) {
		return new StringKey(value);
	}

	@Override
	protected String extractIndexValueFromKey(BlueKey indexKey) {
		if(indexKey instanceof StringKey) {
			return ((StringKey)indexKey).getId();
		}
		return null;
	}

}
