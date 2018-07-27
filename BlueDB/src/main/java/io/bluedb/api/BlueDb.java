package io.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueDb {

	public <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, Class<?>... additionalClassesToRegister) throws BlueDbException;

	public <T extends Serializable> BlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException;

	public void backup(Path path) throws BlueDbException;

	public void shutdown() throws BlueDbException;
}
