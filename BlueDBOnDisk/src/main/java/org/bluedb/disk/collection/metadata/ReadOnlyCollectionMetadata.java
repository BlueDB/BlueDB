package org.bluedb.disk.collection.metadata;

import java.nio.file.Path;

import org.bluedb.disk.config.ConfigurationService;
import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.file.ReadOnlyFileManager;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;

public class ReadOnlyCollectionMetadata extends ReadableCollectionMetadata {

	ReadOnlyFileManager fileManager;

	public ReadOnlyCollectionMetadata(Path collectionPath, ConfigurationService configurationService, EncryptionServiceWrapper encryptionService) {
		super(collectionPath);
		// meta data needs its own serializer because collection doesn't know which classes to register until metadata deserializes them from disk
		BlueSerializer serializer = new ThreadLocalFstSerializer(configurationService);
		fileManager = new ReadOnlyFileManager(serializer, encryptionService);  
	}

	@Override
	public ReadOnlyFileManager getFileManager() {
		return fileManager;
	}
}
