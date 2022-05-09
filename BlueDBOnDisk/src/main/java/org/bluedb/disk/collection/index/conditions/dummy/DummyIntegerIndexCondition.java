package org.bluedb.disk.collection.index.conditions.dummy;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.IntegerIndexCondition;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.ValueKey;

public class DummyIntegerIndexCondition<T extends Serializable> implements IntegerIndexCondition, OnDiskDummyIndexCondition<Integer, T> {
	private final Class<T> indexedCollectionType;
	
	public DummyIntegerIndexCondition(Class<T> indexedCollectionType) {
		this.indexedCollectionType = indexedCollectionType;
	}
	
	@Override
	public Class<T> getIndexedCollectionType() {
		return indexedCollectionType;
	}

	@Override
	public Class<? extends ValueKey> getIndexKeyType() {
		return IntegerKey.class;
	}

	@Override
	public IntegerIndexCondition isEqualTo(Integer value) {
		return this;
	}

	@Override
	public IntegerIndexCondition isIn(BlueSimpleSet<Integer> values) {
		return this;
	}

	@Override
	public IntegerIndexCondition meets(Condition<Integer> condition) {
		return this;
	}
	
	@Override
	public IntegerIndexCondition isInRange(int minValue, int maxValue) {
		return this;
	}

	@Override
	public IntegerIndexCondition isLessThan(int value) {
		return this;
	}

	@Override
	public IntegerIndexCondition isLessThanOrEqualTo(int value) {
		return this;
	}

	@Override
	public IntegerIndexCondition isGreaterThan(int value) {
		return this;
	}

	@Override
	public IntegerIndexCondition isGreaterThanOrEqualTo(int value) {
		return this;
	}

}
