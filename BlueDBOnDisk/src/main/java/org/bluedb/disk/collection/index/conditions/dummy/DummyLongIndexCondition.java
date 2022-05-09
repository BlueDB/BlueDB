package org.bluedb.disk.collection.index.conditions.dummy;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.LongIndexCondition;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.ValueKey;

public class DummyLongIndexCondition<T extends Serializable> implements LongIndexCondition, OnDiskDummyIndexCondition<Long, T> {
	private final Class<T> indexedCollectionType;
	
	public DummyLongIndexCondition(Class<T> indexedCollectionType) {
		this.indexedCollectionType = indexedCollectionType;
	}
	
	@Override
	public Class<T> getIndexedCollectionType() {
		return indexedCollectionType;
	}

	@Override
	public Class<? extends ValueKey> getIndexKeyType() {
		return LongKey.class;
	}

	@Override
	public LongIndexCondition isEqualTo(Long value) {
		return this;
	}

	@Override
	public LongIndexCondition isIn(BlueSimpleSet<Long> values) {
		return this;
	}
	
	@Override
	public LongIndexCondition isInRange(long minValue, long maxValue) {
		return this;
	}

	@Override
	public LongIndexCondition meets(Condition<Long> condition) {
		return this;
	}

	@Override
	public LongIndexCondition isLessThan(long value) {
		return this;
	}

	@Override
	public LongIndexCondition isLessThanOrEqualTo(long value) {
		return this;
	}

	@Override
	public LongIndexCondition isGreaterThan(long value) {
		return this;
	}

	@Override
	public LongIndexCondition isGreaterThanOrEqualTo(long value) {
		return this;
	}

}
