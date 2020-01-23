package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

import org.bluedb.disk.file.ReadOnlyFileManager;

public class ReadOnlySegment <T extends Serializable> extends ReadableSegment<T> {

	private final ReadOnlyFileManager fileManager;

//	protected static <T extends Serializable> ReadOnlySegment<T> getTestSegment () {
//		return new ReadOnlySegment<T>();
//	}
//
//	protected ReadOnlySegment() {super(null, null, null); fileManager = null; }
//
	public ReadOnlySegment(Path segmentPath, Range segmentRange, ReadOnlyFileManager fileManager, final List<Long> rollupLevels) {
		super(segmentPath, segmentRange, rollupLevels);
		this.fileManager = fileManager;
	}

	@Override
	protected ReadOnlyFileManager getFileManager() {
		return fileManager;
	}
}
