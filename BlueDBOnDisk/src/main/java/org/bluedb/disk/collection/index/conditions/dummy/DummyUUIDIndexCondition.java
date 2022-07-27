package org.bluedb.disk.collection.index.conditions.dummy;

import java.io.Serializable;
import java.util.UUID;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.UUIDIndexCondition;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;

public class DummyUUIDIndexCondition<T extends Serializable> implements UUIDIndexCondition, OnDiskDummyIndexCondition<UUID, T> {
	private final Class<T> indexedCollectionType;
	
	public DummyUUIDIndexCondition(Class<T> indexedCollectionType) {
		this.indexedCollectionType = indexedCollectionType;
	}

	@Override
	public Class<? extends ValueKey> getIndexKeyType() {
		return UUIDKey.class;
	}
	
	@Override
	public Class<T> getIndexedCollectionType() {
		return indexedCollectionType;
	}

	@Override
	public UUIDIndexCondition isEqualTo(UUID value) {
		return this;
	}

	@Override
	public UUIDIndexCondition isIn(BlueSimpleSet<UUID> values) {
		return this;
	}

	@Override
	public UUIDIndexCondition meets(Condition<UUID> condition) {
		return this;
	}

}
