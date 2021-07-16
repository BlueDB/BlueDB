package org.bluedb.disk.file;

import org.bluedb.disk.encryption.EncryptionServiceWrapper;
import org.bluedb.disk.serialization.BlueSerializer;

public class ReadOnlyFileManager  extends ReadFileManager {

	public ReadOnlyFileManager(BlueSerializer serializer, EncryptionServiceWrapper encryptionService) {
		super(serializer, encryptionService);
	}
}
