package io.bluedb.disk.segment.writer;

import java.io.Serializable;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.file.BlueObjectOutput;
import io.bluedb.disk.serialization.BlueEntity;

public interface StreamingWriter<X extends Serializable> {
	public void process(BlueObjectInput<BlueEntity<X>> input, BlueObjectOutput<BlueEntity<X>> output) throws BlueDbException;
}