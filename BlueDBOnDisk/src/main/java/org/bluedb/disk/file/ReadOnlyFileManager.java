package org.bluedb.disk.file;

import org.bluedb.disk.serialization.BlueSerializer;

public class ReadOnlyFileManager  extends ReadFileManager {

	public ReadOnlyFileManager(BlueSerializer serializer) {
		super(serializer);
	}
}
