package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.BlueTimeQuery;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadWriteBlueDbOnDisk;
import org.bluedb.disk.query.BlueTimeQueryOnDisk;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadWriteBlueTimeCollectionOnDisk<T extends Serializable> extends ReadWriteBlueCollectionOnDisk<T> implements BlueTimeCollection<T> {

	public ReadWriteBlueTimeCollectionOnDisk(ReadWriteBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
	}

	@Override
	public <I extends ValueKey> BlueIndex<I, T> createIndex(String name, Class<I> keyType, KeyExtractor<I, T> keyExtractor) throws BlueDbException {
		return indexManager.getOrCreate(name, keyType, keyExtractor);
	}

	@Override
	public BlueTimeQuery<T> query() {
		return new BlueTimeQueryOnDisk<T>(this);
	}

}
