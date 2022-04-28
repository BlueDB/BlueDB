package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;
import org.bluedb.disk.serialization.BlueEntity;

public class OnDiskStartTimeIndexCondition<T extends Serializable> extends OnDiskLongIndexCondition<T> {
	public OnDiskStartTimeIndexCondition(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		super(index);
	}
	
	@Override
	protected List<? extends ValueKey> extractIndexKeysFromEntity(BlueEntity<T> entity) {
		List<ValueKey> indexKeys = new LinkedList<>();
		ValueKey startTimeIndexKey = extractStartTimeIndexKeyFromEntity(entity);
		if(startTimeIndexKey != null) {
			indexKeys.add(startTimeIndexKey);
		}
		return indexKeys;
	}
	
	protected ValueKey extractStartTimeIndexKeyFromEntity(BlueEntity<T> entity) {
		BlueKey key = entity.getKey();
		if(key instanceof TimeKey) {
			return new LongKey(((TimeKey)key).getTime());
		}
		return null;
	}
}
