package org.bluedb.disk.collection.index.conditions;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.ReadableIndexOnDisk;
import org.bluedb.disk.serialization.BlueEntity;

public class OnDiskEndTimeIndexCondition<T extends Serializable> extends OnDiskLongIndexCondition<T> {
	public OnDiskEndTimeIndexCondition(ReadableIndexOnDisk<? extends ValueKey, T> index) {
		super(index);
	}
	
	@Override
	protected List<? extends ValueKey> extractIndexKeysFromEntity(BlueEntity<T> entity) {
		List<ValueKey> indexKeys = new LinkedList<>();
		ValueKey startTimeIndexKey = extractEndTimeIndexKeyFromEntity(entity);
		if(startTimeIndexKey != null) {
			indexKeys.add(startTimeIndexKey);
		}
		return indexKeys;
	}

	protected ValueKey extractEndTimeIndexKeyFromEntity(BlueEntity<T> entity) {
		BlueKey key = entity.getKey();
		if(key instanceof TimeFrameKey) {
			return new LongKey(((TimeFrameKey)key).getEndTime());
		}
		return null;
	}
}
