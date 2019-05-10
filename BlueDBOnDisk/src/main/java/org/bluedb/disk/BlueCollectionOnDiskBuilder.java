package org.bluedb.disk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.bluedb.api.BlueCollectionBuilder;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.BlueCollectionOnDisk;

public class BlueCollectionOnDiskBuilder<T extends Serializable> implements BlueCollectionBuilder<T>{

	private final BlueDbOnDisk db;
	private final Class<T> valueType;
	private final Class<? extends BlueKey> requestedKeyType;
	String name;
	ArrayList<Class<? extends Serializable>> registeredClasses = new ArrayList<>();

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

	@Override
	public BlueCollectionOnDiskBuilder<T> usingClasses(Collection<Class<? extends Serializable>> classesToRegister) {
		registeredClasses.addAll(classesToRegister);
		return this;
	}

	@Override
	public BlueCollectionOnDisk<T> build() throws BlueDbException {
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] registeredClassesArray = (Class<? extends Serializable>[]) new Class[registeredClasses.size()];
		registeredClasses.toArray(registeredClassesArray);
		BlueCollectionOnDisk<T> collection = (BlueCollectionOnDisk<T>) db.initializeCollection(name, requestedKeyType, valueType, registeredClassesArray);
		return collection;
	}
}
