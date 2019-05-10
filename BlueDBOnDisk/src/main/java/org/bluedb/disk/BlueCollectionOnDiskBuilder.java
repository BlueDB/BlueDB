package org.bluedb.disk;

import java.io.Serializable;

import org.bluedb.api.BlueCollectionBuilder;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;

public class BlueCollectionOnDiskBuilder<T extends Serializable> implements BlueCollectionBuilder<T>{

	private final BlueDbOnDisk db;
	private final Class<T> valueType;
	private final Class<? extends BlueKey> requestedKeyType;
	String name;
	@SuppressWarnings("unchecked")
	Class<? extends Serializable>[] additionalRegisteredClasses = (Class<? extends Serializable>[]) new Class[] {};

	protected BlueCollectionOnDiskBuilder(BlueDbOnDisk db, Class<? extends BlueKey> keyType, Class<T> valueType) {
		this.db = db;
		this.requestedKeyType = keyType;
		this.valueType = valueType;
	}

	@Override
	public BlueCollectionOnDiskBuilder<T> withName(String name) {
		this.name = name;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public BlueCollectionOnDiskBuilder<T> withRegisteredClasses(Class<? extends Serializable>... additionalRegisteredClasses) {
		this.additionalRegisteredClasses = additionalRegisteredClasses;
		return this;
	}

	@Override
	public BlueCollectionOnDisk<T> build() throws BlueDbException {
		BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) db.initializeCollection(name, requestedKeyType, valueType, additionalRegisteredClasses);
		return collection;
	}
}
