package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;

import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.rollup.Rollupable;

public class Segment <T extends Serializable> extends ReadableSegment<T> {

	public Segment(Path segmentPath, Range segmentRange, Rollupable rollupable, FileManager fileManager, final List<Long> rollupLevels) {
		super(segmentPath, segmentRange, rollupable, fileManager, rollupLevels);
	}

}
