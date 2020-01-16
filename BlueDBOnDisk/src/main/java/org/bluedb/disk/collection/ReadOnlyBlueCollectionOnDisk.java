package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.ReadOnlyBlueDbOnDisk;
import org.bluedb.disk.collection.metadata.ReadOnlyCollectionMetadata;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadOnlyBlueCollectionOnDisk<T extends Serializable> extends ReadableBlueCollectionOnDisk<T> {

	private ReadOnlyCollectionMetadata metadata;

	public ReadOnlyBlueCollectionOnDisk(ReadOnlyBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		this(db, name, requestedKeyType, valueType, additionalRegisteredClasses, null);
	}

	public ReadOnlyBlueCollectionOnDisk(ReadOnlyBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
		metadata = getOrCreateMetadata();
	}

	@Override
	public ReadOnlyCollectionMetadata getMetaData() {
		return metadata;
	}

	@Override
	protected ReadOnlyCollectionMetadata getOrCreateMetadata() {
		if (metadata == null) {
			metadata = new ReadOnlyCollectionMetadata(getPath());
		}
		return metadata;
	}

	@Override
	protected Class<? extends Serializable>[] getClassesToRegister(Class<? extends BlueKey> requestedKeyType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		// NOTE: don't need additionalRegisteredClasses since we only care about already serialized classes.
		List<Class<? extends Serializable>> classesToRegister = metadata.getSerializedClassList();
		@SuppressWarnings("unchecked")
		Class<? extends Serializable>[] returnValue = classesToRegister.toArray(new Class[classesToRegister.size()]);
		return returnValue;
	}
}
