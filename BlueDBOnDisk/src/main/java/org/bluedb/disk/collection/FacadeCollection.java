package org.bluedb.disk.collection;

import java.io.Serializable;

import org.bluedb.api.ReadBlueQuery;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadableBlueDbOnDisk;

public class FacadeCollection<T extends Serializable> implements ReadableBlueCollection<T> {

	private final ReadableBlueDbOnDisk db;
	private final Class<T> valueType;
	private final String name;
	private final DummyReadOnlyBlueCollectionOnDisk<T> dummyCollection;

	public FacadeCollection(ReadableBlueDbOnDisk db, String name, Class<T> valueType) {
		this.db = db;
		this.valueType = valueType;
		this.name = name;
		this.dummyCollection = new DummyReadOnlyBlueCollectionOnDisk<T>();
	}

	private ReadableBlueCollection<T> getCollection() {
		if (db.collectionFolderExists(name)) {
			try {
				return db.getCollection(name, valueType);
			} catch (BlueDbException e) {
				return dummyCollection;
			}
		} else {
			return dummyCollection;
		}
	}

	@Override
	public <K extends ValueKey> BlueIndex<K, T> getIndex(String name, Class<K> keyType) throws BlueDbException {
		return getCollection().getIndex(name, keyType);
	}

	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		return getCollection().contains(key);
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		return getCollection().get(key);
	}

	@Override
	public BlueKey getLastKey() {
		return getCollection().getLastKey();
	}

	@Override
	public ReadBlueQuery<T> query() {
		return getCollection().query();
	}


}
