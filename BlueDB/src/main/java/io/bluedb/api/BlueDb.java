package io.bluedb.api;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;

public interface BlueDb {
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type);

	public void shutdown() throws BlueDbException;
}
