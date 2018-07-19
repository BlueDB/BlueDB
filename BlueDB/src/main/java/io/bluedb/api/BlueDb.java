package io.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;
import io.bluedb.api.exceptions.BlueDbException;

public interface BlueDb {
	public <T extends Serializable> BlueCollection<T> getCollection(Class<T> type, String name) throws BlueDbException;

	public void backup(Path path) throws BlueDbException;

	public void shutdown() throws BlueDbException;
}
