package io.bluedb.disk.recovery;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.segment.BlueEntity;
import io.bluedb.disk.segment.Segment;

public class RecoveryManager {
	
	private static String SUFFIX = ".pending";

	private final BlueCollectionImpl collection;
	private final Path path;

	public RecoveryManager(BlueCollectionImpl db) {
		this.collection = db;
		this.path = db.getPath();
	}

	public void saveChange(PendingChange change) throws BlueDbException {
		String filename = getFileName(change);
		Blutils.save(filename, change);
	}

	public void removeChange(PendingChange change) throws BlueDbException {
		String filename = getFileName(change);
		File file = new File(filename);
		if (!file.delete()) {
			// TODO do we want to throw an exception
			throw new BlueDbException("failed to remove pending change from recovery folder: " + change);
		}
	}

	public static String getFileName(PendingChange change) {
		return  String.valueOf(change.getTimeCreated()) + SUFFIX;
	}

	public List<PendingChange> getPendingChanges() {
		List<File> pendingChangeFiles = Blutils.listFiles(path, SUFFIX);
		List<PendingChange> changes = new ArrayList<>();
		for (File file: pendingChangeFiles) {
			// TODO also remember to throw out corrupted files
		}
		return changes;
	}


	public void recover() {
		List<PendingChange> pendingChanges = getPendingChanges();
		for (PendingChange change: pendingChanges) {
			BlueKey key = change.getKey();
			List<Segment> segments = collection.getSegments(key);
			for (Segment segment: segments) {
//				change.applyChange(segment);
			}
//			recoveryManager.removeChange(change);
		}
	}
}
