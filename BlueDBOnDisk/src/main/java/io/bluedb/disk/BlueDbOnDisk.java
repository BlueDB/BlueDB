package io.bluedb.disk;

import java.io.Serializable;
import io.bluedb.api.BlueDb;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.BlueCollection;

public class BlueDbOnDisk implements BlueDb {

	// TODO figure out directory
	
	@Override
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type) {
		return new BlueCollectionImpl<>(type);
	}

	@Override
	public void shutdown() throws BlueDbException {
		// TODO Auto-generated method stub
	}

}
