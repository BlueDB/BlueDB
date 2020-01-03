package org.bluedb.disk.segment.writer;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.validation.SerializationException;

public class BatchWriter<T extends Serializable> implements StreamingWriter<T> {

	LinkedList<IndividualChange<T>> changes;

	public BatchWriter(Collection<IndividualChange<T>> changes) {
		this.changes = new LinkedList<IndividualChange<T>>(changes);
	}

	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		while (input.hasNext() && !changes.isEmpty()) {
			BlueKey peekFromInput = input.peek().getKey();
			BlueKey peekFromChanges = changes.peek().getKey();
			
			if (peekFromInput.equals(peekFromChanges)) {
				replaceItemWithNextChange(input.nextWithoutDeserializing(), changes, output);
			} else if (peekFromInput.compareTo(peekFromChanges) > 0) {
				writeNextChange(changes, output);
			} else {
				output.writeBytes(input.nextWithoutDeserializing());
			}
		}
		
		// drain out the remaining items from whichever is not empty
		while (input.hasNext()) {
			output.writeBytes(input.nextWithoutDeserializing());
		}
		while (!changes.isEmpty()) {
			writeNextChange(changes, output);
		}
	}

	private static <T extends Serializable> void writeNextChange(LinkedList<IndividualChange<T>> changes, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		replaceItemWithNextChange(null, changes, output);
	}

	private static <T extends Serializable> void replaceItemWithNextChange(byte[] originalItemBytes, LinkedList<IndividualChange<T>> changes, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		BlueEntity<T> newEntity = changes.poll().getNewEntity();
		
		if(newEntity != null) {
			try {
				output.write(newEntity);
			} catch (SerializationException e) {
				//Don't let a single item failing to serialize stop the rest of the changes in the batch
				
				if(originalItemBytes != null) {
					new BlueDbException("A BlueDB batch query was supposed to replace an object but the replacement object failed to serialize. The object will remain unchanged. Key: " + newEntity.getKey(), e).printStackTrace();
					output.writeBytes(originalItemBytes);
				} else {
					new BlueDbException("A BlueDB batch query was supposed to insert an object but failed to serialize it. Key: " + newEntity.getKey(), e).printStackTrace();
				}
			}
		} //else - if newItem is null then the item is effectively deleted by not writing it to the output
	}
}
