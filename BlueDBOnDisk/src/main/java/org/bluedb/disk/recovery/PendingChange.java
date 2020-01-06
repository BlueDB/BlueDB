package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;
import org.bluedb.disk.collection.index.IndexManager;
import org.bluedb.disk.segment.Segment;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

public class PendingChange<T extends Serializable> implements Serializable, Recoverable<T> {

	private static final long serialVersionUID = 1L;

	private BlueKey key;
	private T oldValue;
	private T newValue;
	private long timeCreated;
	private long recoverableId;
	
	private PendingChange(BlueKey key, T oldValue, T newValue) {
		this.key = key;
		this.oldValue = oldValue;
		this.newValue = newValue;
		timeCreated = System.currentTimeMillis();
	}
	
	public static <T extends Serializable> PendingChange<T> createDelete(BlueKey key, T value){
		return new PendingChange<T>(key, value, null);
	}

	public static <T extends Serializable> PendingChange<T> createInsert(BlueKey key, T value, BlueSerializer serializer) throws SerializationException {
		T newValue = serializer.clone(value);
		return new PendingChange<T>(key, null, newValue);
	}

	public static <T extends Serializable> PendingChange<T> createUpdate(BlueKey key, T value, Updater<T> updater, BlueSerializer serializer) throws SerializationException {
		T oldValue = serializer.clone(value);
		T newValue = serializer.clone(oldValue);
		updater.update(newValue);
		return new PendingChange<T>(key, oldValue, newValue);
	}

	public static <T extends Serializable> PendingChange<T> createUpdate(BlueEntity<T> entity, Mapper<T> mapper, BlueSerializer serializer) throws SerializationException {
		BlueKey key = entity.getKey();
		T value = entity.getValue();
		return createUpdate(key, value, mapper, serializer);
	}

	public static <T extends Serializable> PendingChange<T> createUpdate(BlueKey key, T value, Mapper<T> mapper, BlueSerializer serializer) throws SerializationException {
		T oldValue = serializer.clone(value);
		T newValue = mapper.update(serializer.clone(oldValue));
		return new PendingChange<T>(key, oldValue, newValue);
	}

	@Override
	public void apply(BlueCollectionOnDisk<T> collection) throws BlueDbException {
		IndexManager<T> indexManager = collection.getIndexManager();
		indexManager.removeFromAllIndexes(key, oldValue);
		List<Segment<T>> segments = collection.getSegmentManager().getAllSegments(key);
		for (Segment<T> segment: segments) {
			applyChange(segment);
		}
		indexManager.addToAllIndexes(key, newValue);
	}

	public void applyChange(Segment<T> segment) throws BlueDbException {
		if (isInsert()) {
			segment.insert(key, newValue);
		} else if (isDelete()) {
			segment.delete(key);
		} else if (isUpdate()) {
			segment.update(key, newValue);
		}
	}

	public BlueKey getKey() {
		return key;
	}
	public T getOldValue() {
		return oldValue;
	}
	public T getNewValue() {
		return newValue;
	}

	@Override
	public long getTimeCreated() {
		return timeCreated;
	}

	public boolean isDelete() {
		return newValue == null;
	}

	public boolean isInsert() {
		return newValue != null && oldValue == null;
	}

	public boolean isUpdate() {
		return newValue != null && oldValue != null;
	}

	@Override
	public String toString() {
		return "<PendingChange for " + key +": " + String.valueOf(oldValue) + "=> " + String.valueOf(newValue) + ">";
	}

	@Override
	public long getRecoverableId() {
		return recoverableId;
	}

	@Override
	public void setRecoverableId(long recoverableId) {
		this.recoverableId = recoverableId;
	}
}
