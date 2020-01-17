package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadableBlueDbOnDisk;
import org.bluedb.disk.collection.index.ReadOnlyBlueIndexOnDisk;
import org.bluedb.disk.collection.index.ReadOnlyIndexManager;
import org.bluedb.disk.collection.metadata.ReadOnlyCollectionMetadata;
import org.bluedb.disk.file.ReadOnlyFileManager;
import org.bluedb.disk.segment.ReadOnlySegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadOnlyBlueCollectionOnDisk<T extends Serializable> extends ReadableBlueCollectionOnDisk<T> {

	private ReadOnlyCollectionMetadata metadata;
	private final ReadOnlyFileManager fileManager;
	private final ReadOnlySegmentManager<T> segmentManager;
	protected final ReadOnlyIndexManager<T> indexManager;

	public ReadOnlyBlueCollectionOnDisk(ReadableBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		this(db, name, requestedKeyType, valueType, additionalRegisteredClasses, null);
	}

	public ReadOnlyBlueCollectionOnDisk(ReadableBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
		metadata = getOrCreateMetadata();
		fileManager = new ReadOnlyFileManager(serializer);
		segmentManager = new ReadOnlySegmentManager<T>(collectionPath, fileManager, segmentSizeSettings.getConfig());
		indexManager = new ReadOnlyIndexManager<T>(this, collectionPath);
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

	@Override
	public ReadOnlyFileManager getFileManager() {
		return fileManager;
	}

	@Override
	public ReadOnlySegmentManager<T> getSegmentManager() {
		return segmentManager;
	}

	@Override
	public <I extends ValueKey> ReadOnlyBlueIndexOnDisk<I, T> getIndex(String indexName, Class<I> keyType)
			throws BlueDbException {
		return indexManager.getIndex(indexName, keyType);
	}
}
