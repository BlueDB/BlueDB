package org.bluedb.disk.segment.writer;

import java.io.Serializable;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.encryption.EncryptionUtils;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.serialization.BlueEntity;

public class DeleteWriter<T extends Serializable> implements StreamingWriter<T> {

	final BlueKey key;

	public DeleteWriter(BlueKey key) {
		this.key = key;
	}

	@Override
	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		boolean shouldSkipEncryption = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(input.getMetadata(), output.getMetadata());
		while (input.hasNext()) {
			BlueEntity<T> entry = input.next();
			if (!entry.getKey().equals(key)) {
				if (shouldSkipEncryption) {
					output.writeBytesAndForceSkipEncryption(input.getLastRawBytes());
				} else {
					output.writeBytesAndAllowEncryption(input.getLastUnencryptedBytes());
				}
			}
		}
	}

}
