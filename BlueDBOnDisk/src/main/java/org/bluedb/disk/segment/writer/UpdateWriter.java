package org.bluedb.disk.segment.writer;

import java.io.Serializable;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.encryption.EncryptionUtils;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.serialization.BlueEntity;

public class UpdateWriter<T extends Serializable> implements StreamingWriter<T> {

	final BlueKey newKey;
	final T newValue;

	public UpdateWriter(BlueKey key, T value) {
		newKey = key;
		newValue = value;
	}

	@Override
	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		boolean shouldSkipEncryption = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(input.getMetadata(), output.getMetadata());
		BlueEntity<T> newEntity = new BlueEntity<T>(newKey, newValue);
		while (input.hasNext()) {
			BlueEntity<T> iterEntity = input.next();
			BlueKey iterKey = iterEntity.getKey();
			if (iterKey.equals(newKey)) {
				output.write(newEntity);
				newEntity = null;
			} else {
				if (shouldSkipEncryption) {
					output.writeBytesAndForceSkipEncryption(input.getLastRawBytes());
				} else {
					output.writeBytes(input.getLastUnencryptedBytes());
				}
			}
		}
	}

}
