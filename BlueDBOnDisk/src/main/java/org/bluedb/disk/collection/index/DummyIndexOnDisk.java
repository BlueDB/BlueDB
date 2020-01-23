package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.ValueKey;

public class DummyIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {

	public DummyIndexOnDisk() {}

	@Override
	public List<T> get(I key) throws BlueDbException {
		return null;
	}

	@Override
	public I getLastKey() {
		return null;
	}
}
