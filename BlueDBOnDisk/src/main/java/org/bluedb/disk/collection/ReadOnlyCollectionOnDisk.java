package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.collection.index.FacadeIndexOnDisk;
import org.bluedb.disk.collection.index.NoSuchIndexException;
import org.bluedb.disk.collection.index.ReadOnlyIndexManager;
import org.bluedb.disk.collection.metadata.ReadOnlyCollectionMetadata;
import org.bluedb.disk.file.ReadOnlyFileManager;
import org.bluedb.disk.segment.ReadOnlySegmentManager;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadOnlyCollectionOnDisk<T extends Serializable> extends ReadableCollectionOnDisk<T> {

	private ReadOnlyCollectionMetadata metadata;
	private final ReadOnlyFileManager fileManager;
	private final ReadOnlySegmentManager<T> segmentManager;
	protected final ReadOnlyIndexManager<T> indexManager;

	public ReadOnlyCollectionOnDisk(ReadableDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		this(db, name, requestedKeyType, valueType, additionalRegisteredClasses, null);
	}

	public ReadOnlyCollectionOnDisk(ReadableDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
		metadata = getOrCreateMetadata();
		fileManager = new ReadOnlyFileManager(serializer, db.getEncryptionService());
		segmentManager = new ReadOnlySegmentManager<>(collectionPath, fileManager, segmentSizeSettings.getConfig());
		indexManager = new ReadOnlyIndexManager<>(this, collectionPath);
	}

	@Override
	protected ReadOnlyCollectionMetadata getOrCreateMetadata() {
		if (metadata == null) {
			metadata = new ReadOnlyCollectionMetadata(getPath(), this.encryptionService);
		}
		return metadata;
	}

	@Override
	protected Class<? extends Serializable>[] getClassesToRegister(List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
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
	public <I extends ValueKey> BlueIndex<I, T> getIndex(String indexName, Class<I> keyType)
			throws BlueDbException {
		try {
			return getExistingIndex(indexName, keyType);
		} catch (NoSuchIndexException e1) {
			return new FacadeIndexOnDisk<>(() -> {
				try {
					return indexManager.getIndex(indexName, keyType);
				} catch (BlueDbException e2) {
					return null;
				}
			});
		}
	}

	public <I extends ValueKey> BlueIndex<I, T> getExistingIndex(String indexName, Class<I> keyType) throws BlueDbException {
		return indexManager.getIndex(indexName, keyType);
	}
}
