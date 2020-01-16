package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;

import org.bluedb.disk.file.FileManager;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;
import org.bluedb.disk.segment.rollup.Rollupable;

public class SegmentManager<T extends Serializable> extends ReadableSegmentManager<T> {

	private final FileManager fileManager;

	private final Rollupable rollupable;

	public SegmentManager(Path collectionPath, FileManager fileManager, Rollupable rollupable, SegmentSizeConfiguration sizeConfig) {
		super(collectionPath, sizeConfig);
		this.fileManager = fileManager;
		this.rollupable = rollupable;
	}

	public FileManager getFileManager() {
		return fileManager;
	}

	public Rollupable getRollupable() {
		return rollupable;
	}

}
