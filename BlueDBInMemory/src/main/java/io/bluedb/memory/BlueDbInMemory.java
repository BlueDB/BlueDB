package io.bluedb.memory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.BlueCollection;

public class BlueDbInMemory implements BlueDb {

	Map<String, BlueCollection<? extends Serializable>> collections = new HashMap<>();
	Map<String, Class<? extends Serializable>> classes = new HashMap<>();

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) {
		synchronized(collections) {
			if (!collections.containsKey(name)) {
				collections.put(name, new BlueCollectionImpl<T>(type));
				classes.put(name, type);
			}
			if (classes.get(name) != type) {
				throw new RuntimeException("Collection '" + name + "' is not for type " + type);
			}
			return (BlueCollection<T>)(collections.get(name));
		}
	}

	@Override
	public void shutdown() throws BlueDbException {
		// TODO Auto-generated method stub
	}

}
