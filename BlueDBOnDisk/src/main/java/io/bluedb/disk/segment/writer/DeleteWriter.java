package io.bluedb.disk.segment.writer;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.serialization.BlueEntity;

public class DeleteWriter<T extends Serializable> implements StreamingWriter<T> {

	final BlueKey key;

	public DeleteWriter(BlueKey key) {
		this.key = key;
	}

	@Override
	public void process(BlueObjectInput<BlueEntity<T>> input, BlueObjectOutput<BlueEntity<T>> output) throws BlueDbException {
		while (input.hasNext()) {
			BlueEntity<T> entry = input.next();
			if (!entry.getKey().equals(key)) {
				output.writeBytes(input.getLastBytes());
			}
		}
	}
}
