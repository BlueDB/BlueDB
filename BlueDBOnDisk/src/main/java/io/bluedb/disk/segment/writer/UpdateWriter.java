package io.bluedb.disk.segment.writer;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.serialization.BlueEntity;

public class UpdateWriter<T extends Serializable> implements StreamingWriter<T> {

	final BlueKey newKey;
	final T newValue;

	public UpdateWriter(BlueKey key, T value) {
		newKey = key;
		newValue = value;
	}

	@Override
	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		BlueEntity<T> newEntity = new BlueEntity<T>(newKey, newValue);
		while (input.hasNext()) {
			BlueEntity<T> iterEntity = input.next();
			BlueKey iterKey = iterEntity.getKey();
			if (iterKey.equals(newKey)) {
				output.write(newEntity);
				newEntity = null;
			} else {
				output.write(iterEntity);
			}
		}
	}
}
