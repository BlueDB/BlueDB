package org.bluedb.disk.collection.index.conditions.dummy;

import java.io.Serializable;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.StringIndexCondition;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.ValueKey;

public class DummyStringIndexCondition<T extends Serializable> implements StringIndexCondition, OnDiskDummyIndexCondition<String, T> {
	private final Class<T> indexedCollectionType;
	
	public DummyStringIndexCondition(Class<T> indexedCollectionType) {
		this.indexedCollectionType = indexedCollectionType;
	}
	
	@Override
	public Class<T> getIndexedCollectionType() {
		return indexedCollectionType;
	}

	@Override
	public Class<? extends ValueKey> getIndexKeyType() {
		return StringKey.class;
	}

	@Override
	public StringIndexCondition isEqualTo(String value) {
		return this;
	}

	@Override
	public StringIndexCondition isIn(BlueSimpleSet<String> values) {
		return this;
	}

	@Override
	public StringIndexCondition meets(Condition<String> condition) {
		return this;
	}

}
