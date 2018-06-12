package io.bluedb.disk;

import java.io.Serializable;
import io.bluedb.api.BlueDb;
import io.bluedb.api.BlueDbCollection;

public class BlueDBOnDisk implements BlueDb {

	@Override
	public <T extends Serializable> BlueDbCollection<T> getCollection(Class<T> type) {
		// TODO Auto-generated method stub
		return null;
	}

}
