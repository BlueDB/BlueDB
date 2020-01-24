package org.bluedb.disk.collection;

import java.io.Serializable;

import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.index.DummyIndexOnDisk;
import org.bluedb.disk.query.DummyQuery;

public class DummyReadOnlyCollectionOnDisk<T extends Serializable> implements ReadableBlueTimeCollection<T> {

	public DummyReadOnlyCollectionOnDisk() {}

	@Override
	public <K extends ValueKey> BlueIndex<K, T> getIndex(String name, Class<K> keyType) throws BlueDbException {
		return new DummyIndexOnDisk<>();
	}

	@Override
	public boolean contains(BlueKey key) throws BlueDbException {
		return false;
	}

	@Override
	public T get(BlueKey key) throws BlueDbException {
		return null;
	}

	@Override
	public BlueKey getLastKey() {
		return null;
	}

	@Override
	public ReadBlueTimeQuery<T> query() {
		return new DummyQuery<T>();
	}


}
