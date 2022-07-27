package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;
import java.util.UUID;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueSimpleSet;
import org.bluedb.api.index.conditions.UUIDIndexCondition;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;

public class OnDiskUUIDIndexCondition<T extends Serializable> extends OnDiskIndexConditionBase<UUID, T> implements UUIDIndexCondition {
	
	public OnDiskUUIDIndexCondition(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		super(index);
	}

	@Override
	public UUIDIndexCondition isEqualTo(UUID value) {
		super.isEqualTo(value);
		return this;
	}
	
	@Override
	public UUIDIndexCondition isIn(BlueSimpleSet<UUID> values) {
		super.isIn(values);
		return this;
	}
	
	@Override
	public UUIDIndexCondition meets(Condition<UUID> condition) {
		super.meets(condition);
		return this;
	}

	@Override
	protected ValueKey createKeyForIndexValue(UUID value) {
		return new UUIDKey(value);
	}

	@Override
	protected UUID extractIndexValueFromKey(BlueKey indexKey) {
		if(indexKey instanceof UUIDKey) {
			return ((UUIDKey)indexKey).getId();
		}
		return null;
	}

}
