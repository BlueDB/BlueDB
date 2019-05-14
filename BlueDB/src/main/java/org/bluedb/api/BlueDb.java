package org.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

public interface BlueDb {
	
	public <T extends Serializable> BlueCollection<T> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<T> valueType, @SuppressWarnings("unchecked") Class<? extends Serializable>... additionalClassesToRegister) throws BlueDbException;

	public <T extends Serializable, K extends BlueKey> BlueCollectionBuilder<T> collectionBuilder(String name, Class<K> keyType, Class<T> valueType);

	public <T extends Serializable> BlueCollection<T> getCollection(String name, Class<T> valueType) throws BlueDbException;

	public void backup(Path path) throws BlueDbException;

	public void shutdown() throws BlueDbException;
}
