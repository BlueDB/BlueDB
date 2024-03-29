package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.Mapper;
import org.bluedb.api.Updater;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.index.ReadWriteIndexManager;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;

@Deprecated
/*
 * The PendingChange object is no longer used, but is still in the codebase in order to support the initial
 * startup after a site updates to a current version of BlueDB.
 */
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
	public void apply(ReadWriteCollectionOnDisk<T> collection) throws BlueDbException {
		ReadWriteIndexManager<T> indexManager = collection.getIndexManager();
		List<ReadWriteSegment<T>> segments = collection.getSegmentManager().getAllSegments(key);
		for (ReadWriteSegment<T> segment: segments) {
			applyChange(segment);
		}
		indexManager.indexChange(key, oldValue, newValue);
	}

	public void applyChange(ReadWriteSegment<T> segment) throws BlueDbException {
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
