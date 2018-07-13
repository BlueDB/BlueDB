package io.bluedb.disk.segment.writer;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.serialization.BlueEntity;

public class InsertWriter<T extends Serializable> implements StreamingWriter<T> {

	final BlueKey newKey;
	final T newValue;

	public InsertWriter(BlueKey key, T value) {
		newKey = key;
		newValue = value;
	}

	@Override
	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		BlueEntity<T> newEntity = new BlueEntity<T>(newKey, newValue);
		BlueEntity<T> toInsert = newEntity;
		long groupingNumber = newKey.getGroupingNumber();
		while (input.hasNext()) {
			BlueEntity<T> iterEntity = input.next();
			BlueKey iterKey = iterEntity.getKey();
			if (iterKey.equals(newKey)) {
				output.write(newEntity);
				toInsert = null;
			} else if (toInsert != null && iterKey.getGroupingNumber() > groupingNumber) {
				output.write(newEntity);
				toInsert = null;
				output.write(iterEntity);
			} else {
				output.write(iterEntity);
			}
		}
		if (toInsert != null) {
			output.write(newEntity);
		}
	}
}
