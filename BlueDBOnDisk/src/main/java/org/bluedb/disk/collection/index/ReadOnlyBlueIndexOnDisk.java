package org.bluedb.disk.collection.index;

import java.io.Serializable;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.collection.ReadableBlueCollectionOnDisk;
import org.bluedb.disk.segment.rollup.Rollupable;

public class ReadOnlyBlueIndexOnDisk<I extends ValueKey, T extends Serializable> extends ReadableBlueIndexOnDisk<I, T> implements BlueIndex<I, T>, Rollupable {

	private ReadOnlyBlueIndexOnDisk(ReadableBlueCollectionOnDisk<T> collection, Path indexPath, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		super(collection, indexPath, keyExtractor);
	}
}
