package io.bluedb.memory;

import java.io.Serializable;
import io.bluedb.api.BlueDb;
import io.bluedb.api.BlueDbCollection;

public class BlueDbInMemory implements BlueDb {

	@Override
	public <T extends Serializable> BlueDbCollection<T> getCollection(Class<T> type) {
		return new BlueDbInMemoryCollection<>(type);
	}

}
