package org.bluedb.disk;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueCollectionBuilder;
import org.bluedb.api.SegmentSize;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class CollectionOnDiskBuilder<K extends BlueKey, T extends Serializable> implements BlueCollectionBuilder<K, T>{

	protected final ReadWriteDbOnDisk db;
	protected final Class<T> valueType;
	protected final Class<? extends BlueKey> requestedKeyType;
	protected final String name;
	protected SegmentSizeSetting segmentSize;
	ArrayList<Class<? extends Serializable>> registeredClasses = new ArrayList<>();

	protected CollectionOnDiskBuilder(ReadWriteDbOnDisk db, String name, Class<K> keyType, Class<T> valueType) {
		this.db = db;
		this.name = name;
		this.requestedKeyType = keyType;
		this.valueType = valueType;
	}

	@Override
	public BlueCollectionBuilder<K, T> withOptimizedClasses(Collection<Class<? extends Serializable>> classesToRegister) {
		registeredClasses.addAll(classesToRegister);
		return this;
	}

	@Override
	public BlueCollectionBuilder<K, T> withSegmentSize(SegmentSize<K> segmentSize) throws BlueDbException {
		this.segmentSize = SegmentSizeSetting.fromUserSelection(segmentSize);
		return this;
	}

	@Override
	public BlueCollection<T> build() throws BlueDbException {
		return db.initializeCollection(name, requestedKeyType, valueType, registeredClasses, segmentSize);
	}
}
