package org.bluedb.disk.collection.index;

import java.io.Serializable;

import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.conditions.IntegerIndexCondition;
import org.bluedb.api.index.conditions.LongIndexCondition;
import org.bluedb.api.index.conditions.StringIndexCondition;
import org.bluedb.api.index.conditions.UUIDIndexCondition;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.conditions.dummy.DummyIntegerIndexCondition;
import org.bluedb.disk.collection.index.conditions.dummy.DummyLongIndexCondition;
import org.bluedb.disk.collection.index.conditions.dummy.DummyStringIndexCondition;
import org.bluedb.disk.collection.index.conditions.dummy.DummyUUIDIndexCondition;

public class DummyIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {
	private final Class<T> indexedCollectionType;
	
	public DummyIndexOnDisk(Class<T> indexedCollectionType) {
		this.indexedCollectionType = indexedCollectionType;
	}

	@Override
	public I getLastKey() {
		return null;
	}

	@Override
	public IntegerIndexCondition createIntegerIndexCondition() throws UnsupportedIndexConditionTypeException {
		return new DummyIntegerIndexCondition<T>(indexedCollectionType);
	}

	@Override
	public LongIndexCondition createLongIndexCondition() throws UnsupportedIndexConditionTypeException {
		return new DummyLongIndexCondition<T>(indexedCollectionType);
	}

	@Override
	public StringIndexCondition createStringIndexCondition() throws UnsupportedIndexConditionTypeException {
		return new DummyStringIndexCondition<T>(indexedCollectionType);
	}

	@Override
	public UUIDIndexCondition createUUIDIndexCondition() throws UnsupportedIndexConditionTypeException {
		return new DummyUUIDIndexCondition<T>(indexedCollectionType);
	}
}
