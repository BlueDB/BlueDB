package io.bluedb.memory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.BlueCollection;

public class BlueDbInMemory implements BlueDb {

	@SuppressWarnings("rawtypes")
	Map<Class, BlueCollection> collections = new HashMap<>();

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type) {
		if (!collections.containsKey(type)) {
			collections.put(type, new BlueCollectionImpl<T>(type));
		}
		return (BlueCollection<T>)(collections.get(type));
	}

	@Override
	public void shutdown() throws BlueDbException {
		// TODO Auto-generated method stub
	}

}
