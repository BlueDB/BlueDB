package org.bluedb.api;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

public interface BlueDb {
	
	public <V extends Serializable> BlueCollection<V> initializeCollection(String name, Class<? extends BlueKey> keyType, Class<V> valueType, @SuppressWarnings("unchecked") Class<? extends Serializable>... additionalClassesToRegister) throws BlueDbException;

	public <K extends BlueKey, V extends Serializable> BlueCollectionBuilder<K, V> collectionBuilder(String name, Class<K> keyType, Class<V> valueType);

	public <V extends Serializable> BlueCollection<V> getCollection(String name, Class<V> valueType) throws BlueDbException;

	public void backup(Path path) throws BlueDbException;

	public void shutdown() throws BlueDbException;

	public void shutdownNow() throws BlueDbException;
	
	public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws BlueDbException;
}
