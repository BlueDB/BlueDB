package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.ValueKey;

public class FacadeIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {

	private final Supplier<BlueIndex<I, T>> source;

	public FacadeIndexOnDisk(Supplier<BlueIndex<I, T>> source) {
		this.source = source;
	}

	private BlueIndex<I, T> get() {
		BlueIndex<I, T> index = source.get();
		if (index != null) {
			return index;
		} else {
			return new DummyIndexOnDisk<>();
		}
	}

	@Override
	public List<T> get(I key) throws BlueDbException {
		return get().get(key);
	}

	@Override
	public I getLastKey() {
		return get().getLastKey();
	}
}
