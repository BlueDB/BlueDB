package org.bluedb.disk.segment.writer;

import java.io.Serializable;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.serialization.BlueEntity;

public interface StreamingWriter<X extends Serializable> {
	public void process(BlueObjectInput<BlueEntity<X>> input, BlueObjectOutput<BlueEntity<X>> output) throws BlueDbException;
}