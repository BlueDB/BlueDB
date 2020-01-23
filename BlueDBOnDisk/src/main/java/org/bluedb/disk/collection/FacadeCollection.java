package org.bluedb.disk.collection;

import java.io.Serializable;

import org.bluedb.api.ReadBlueQuery;
import org.bluedb.api.ReadableBlueCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.collection.index.FacadeIndexOnDisk;

public class FacadeCollection<T extends Serializable> implements ReadableBlueCollection<T> {

	protected final ReadableDbOnDisk db;
	protected final Class<T> valueType;
	protected final String name;
	protected final DummyReadOnlyCollectionOnDisk<T> dummyCollection;

	public FacadeCollection(ReadableDbOnDisk db, String name, Class<T> valueType) {
		this.db = db;
		this.valueType = valueType;
		this.name = name;
		this.dummyCollection = new DummyReadOnlyCollectionOnDisk<T>();
	}

	
	protected ReadableBlueCollection<T> getCollection() {
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
	public <K extends ValueKey> BlueIndex<K, T> getIndex(String indexName, Class<K> indexKeyType) throws BlueDbException {
		return new FacadeIndexOnDisk<K, T>(() -> {
			try {
				ReadOnlyCollectionOnDisk<T> collection = (ReadOnlyCollectionOnDisk<T>) db.getExistingCollection(name, valueType);
				return collection.getExistingIndex(indexName, indexKeyType);
			} catch (BlueDbException e) {
				return null;
			}
		});
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
