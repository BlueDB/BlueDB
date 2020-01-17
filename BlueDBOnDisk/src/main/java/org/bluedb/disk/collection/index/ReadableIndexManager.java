package org.bluedb.disk.collection.index;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.ValueKey;

public abstract class ReadableIndexManager<T extends Serializable> {

	protected static final String INDEXES_SUBFOLDER = ".index";

	public abstract <K extends ValueKey> ReadableBlueIndexOnDisk<K, T> getIndex(String indexName, Class<K> keyType) throws BlueDbException;

}
