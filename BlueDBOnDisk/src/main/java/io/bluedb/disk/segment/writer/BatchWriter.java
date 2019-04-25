package io.bluedb.disk.segment.writer;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.recovery.IndividualChange;
import io.bluedb.disk.serialization.BlueEntity;

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
				input.next(); // this is the value that is being replaced or deleted so throw it out
				pollOneChangeAndWrite(changes, output);
			} else if (peekFromInput.compareTo(peekFromChanges) > 0) {
				pollOneChangeAndWrite(changes, output);
			} else {
				output.write(input.next());
			}
		}
		// drain out the remaining items from whichever is not empty
		while (input.hasNext()) {
			output.write(input.next());
		}
		while (!changes.isEmpty()) {
			pollOneChangeAndWrite(changes, output);
		}
	}

	private static <T extends Serializable> void pollOneChangeAndWrite(LinkedList<IndividualChange<T>> changes, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		BlueEntity<T> newEntity = changes.poll().getNewEntity();
		if (newEntity != null) {
			output.write(newEntity);
		} // else it's a delete anyway
	}
}
