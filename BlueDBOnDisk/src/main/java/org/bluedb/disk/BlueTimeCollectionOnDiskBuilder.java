package org.bluedb.disk;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.BlueTimeCollectionBuilder;
import org.bluedb.api.SegmentSize;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;

public class BlueTimeCollectionOnDiskBuilder<K extends BlueKey, T extends Serializable> extends BlueCollectionOnDiskBuilder<K, T> implements BlueTimeCollectionBuilder<K, T> {

	public BlueTimeCollectionOnDiskBuilder(BlueDbOnDisk db, String name, Class<K> keyType, Class<T> valueType) {
		super(db, name, keyType, valueType);
	}

	@Override
	public BlueTimeCollectionBuilder<K, T> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister) {
		return (BlueTimeCollectionBuilder<K,T>) super.withOptimizedClasses(classesToRegister);
	}

	@Override
	public BlueTimeCollectionBuilder<K, T> withSegmentSize(SegmentSize<K> segmentSize) throws BlueDbException {
		return (BlueTimeCollectionBuilder<K,T>) super.withSegmentSize(segmentSize);
	}

	@Override
	public BlueTimeCollection<T> build() throws BlueDbException {
		return db.initializeTimeCollection(name, requestedKeyType, valueType, registeredClasses, segmentSize);
	}
}
