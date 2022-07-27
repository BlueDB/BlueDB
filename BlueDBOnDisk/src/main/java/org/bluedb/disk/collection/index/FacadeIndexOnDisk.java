package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.util.function.Supplier;

import org.bluedb.api.exceptions.UnsupportedIndexConditionTypeException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.conditions.IntegerIndexCondition;
import org.bluedb.api.index.conditions.LongIndexCondition;
import org.bluedb.api.index.conditions.StringIndexCondition;
import org.bluedb.api.index.conditions.UUIDIndexCondition;
import org.bluedb.api.keys.ValueKey;

public class FacadeIndexOnDisk<I extends ValueKey, T extends Serializable> implements BlueIndex<I, T> {

	private final Supplier<BlueIndex<I, T>> source;
	private final Class<T> indexedCollectionType;

	public FacadeIndexOnDisk(Supplier<BlueIndex<I, T>> source, Class<T> indexedCollectionType) {
		this.source = source;
		this.indexedCollectionType = indexedCollectionType;
	}

	private BlueIndex<I, T> get() {
		BlueIndex<I, T> index = source.get();
		if (index != null) {
			return index;
		} else {
			return new DummyIndexOnDisk<>(indexedCollectionType);
		}
	}

	@Override
	public I getLastKey() {
		return get().getLastKey();
	}

	@Override
	public IntegerIndexCondition createIntegerIndexCondition() throws UnsupportedIndexConditionTypeException {
		return get().createIntegerIndexCondition();
	}

	@Override
	public LongIndexCondition createLongIndexCondition() throws UnsupportedIndexConditionTypeException {
		return get().createLongIndexCondition();
	}

	@Override
	public StringIndexCondition createStringIndexCondition() throws UnsupportedIndexConditionTypeException {
		return get().createStringIndexCondition();
	}

	@Override
	public UUIDIndexCondition createUUIDIndexCondition() throws UnsupportedIndexConditionTypeException {
		return get().createUUIDIndexCondition();
	}
}
