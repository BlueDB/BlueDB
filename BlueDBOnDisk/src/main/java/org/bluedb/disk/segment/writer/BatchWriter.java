package org.bluedb.disk.segment.writer;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.encryption.EncryptionUtils;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.validation.SerializationException;

public class BatchWriter<T extends Serializable> implements StreamingWriter<T> {

	private SortedChangeSupplier<T> sortedChanges; 
	private Range range;

	public BatchWriter(SortedChangeSupplier<T> sortedChanges, Range range) {
		this.sortedChanges = sortedChanges;
		this.range = range;
	}

	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		boolean shouldSkipEncryptionForUnchangedData = EncryptionUtils.shouldWriterSkipEncryptionForUnchangedDataUsingRawBytes(input.getMetadata(), output.getMetadata());
		while (input.hasNext() && sortedChanges.nextChangeOverlapsRange(range)) {
			BlueKey peekFromInput = input.peek().getKey();
			BlueKey peekFromChanges = sortedChanges.getNextChange().get().getKey();

			if (peekFromInput.equals(peekFromChanges)) {
				if (shouldSkipEncryptionForUnchangedData) {
					replaceItemWithNextChange(input.nextRawBytesWithoutDeserializing(), output, true);
				} else {
					replaceItemWithNextChange(input.nextUnencryptedBytesWithoutDeserializing(), output, false);
				}
			} else if (peekFromInput.compareTo(peekFromChanges) > 0) {
				writeNextChange(output);
			} else {
				if (shouldSkipEncryptionForUnchangedData) {
					output.writeBytesAndForceSkipEncryption(input.nextRawBytesWithoutDeserializing());
				} else {
					output.writeBytesAndAllowEncryption(input.nextUnencryptedBytesWithoutDeserializing());
				}
			}
		}

		// drain out the remaining items from whichever is not empty
		while (input.hasNext()) {
			if (shouldSkipEncryptionForUnchangedData) {
				output.writeBytesAndForceSkipEncryption(input.nextRawBytesWithoutDeserializing());
			} else {
				output.writeBytesAndAllowEncryption(input.nextUnencryptedBytesWithoutDeserializing());
			}
		}
		while (sortedChanges.nextChangeOverlapsRange(range)) {
			writeNextChange(output);
		}
	}

	private void writeNextChange(BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		replaceItemWithNextChange(null, output, false);
	}

	private void replaceItemWithNextChange(byte[] originalItemBytes, BlueObjectOutput<BlueEntity<T>> output, boolean shouldSkipEncryptionForUnchangedData) throws BlueDbException {
		BlueEntity<T> newEntity = sortedChanges.getNextChange().get().getNewEntity();
		sortedChanges.seekToNextChangeInRange(range);

		if (newEntity != null) {
			try {
				output.write(newEntity);
			} catch (SerializationException e) {
				//Don't let a single item failing to serialize stop the rest of the changes in the batch

				if (originalItemBytes != null) {
					new BlueDbException("A BlueDB batch query was supposed to replace an object but the replacement object failed to serialize. The object will remain unchanged. Key: " + newEntity.getKey(), e).printStackTrace();
					if (shouldSkipEncryptionForUnchangedData) {
						output.writeBytesAndForceSkipEncryption(originalItemBytes);
					} else {
						output.writeBytesAndAllowEncryption(originalItemBytes);
					}
				} else {
					new BlueDbException("A BlueDB batch query was supposed to insert an object but failed to serialize it. Key: " + newEntity.getKey(), e).printStackTrace();
				}
			}
		} //else - if newItem is null then the item is effectively deleted by not writing it to the output
	}

}
