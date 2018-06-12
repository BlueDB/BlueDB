package io.bluedb.api;

import java.io.Serializable;

public interface BlueDb {
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type);
}
