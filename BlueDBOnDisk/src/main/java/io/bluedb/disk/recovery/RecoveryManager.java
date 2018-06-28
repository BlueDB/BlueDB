package io.bluedb.disk.recovery;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.bluedb.api.Updater;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.serialization.BlueSerializer;

public class RecoveryManager<T extends Serializable> {
	
	private static String SUFFIX = ".pending";

	private final BlueCollectionImpl<T> collection;
	private final Path recoveryPath;
	private final FileManager fileManager;
	private final BlueSerializer serializer;
	
	public RecoveryManager(BlueCollectionImpl<T> collection, FileManager fileManager, BlueSerializer serializer) {
		this.collection = collection;
		this.fileManager = fileManager;
		this.serializer = serializer;
		this.recoveryPath = Paths.get(collection.getPath().toString(), ".pending");
	}

	public PendingChange<T> saveUpdate(BlueKey key, T originalValue, Updater<T> updater) throws BlueDbException {
		T oldValue = serializer.clone(originalValue);
		T newValue = serializer.clone(originalValue);
		updater.update(newValue);
		PendingChange<T> change = PendingChange.createUpdate(key, oldValue, newValue);
		saveChange(change);
		return change;
	}

	public PendingChange<T> saveDelete(BlueKey key) throws BlueDbException {
		PendingChange<T> change = PendingChange.createDelete(key);
		saveChange(change);
		return change;
	}

	public PendingChange<T> saveInsert(BlueKey key, T value) throws BlueDbException {
		T insertValue = serializer.clone(value);
		PendingChange<T> change = PendingChange.createInsert(key, insertValue);
		saveChange(change);
		return change;
	}

	public void saveChange(PendingChange<T> change) throws BlueDbException {
		String filename = getFileName(change);
		Path path = Paths.get(recoveryPath.toString(), filename);
		fileManager.saveObject(path, change);
	}

	public void removeChange(PendingChange<T> change) throws BlueDbException {
		String filename = getFileName(change);
		Path path = Paths.get(recoveryPath.toString(), filename);
		File file = new File(path.toString());
		file.delete();
	}

	public static String getFileName(PendingChange<?> change) {
		return  String.valueOf(change.getTimeCreated()) + SUFFIX;
	}

	public List<PendingChange<T>> getPendingChanges() {
		List<File> pendingChangeFiles = fileManager.listFiles(recoveryPath, SUFFIX);
		List<PendingChange<T>> changes = new ArrayList<>();
		for (File file: pendingChangeFiles) {
			try {
				@SuppressWarnings("unchecked")
				PendingChange<T> change = (PendingChange<T>) fileManager.loadObject(file.toPath());
				changes.add(change);
			} catch (BlueDbException e) {
				e.printStackTrace();
				file.delete();
			}
		}
		return changes;
	}


	public void recover() {
		List<PendingChange<T>> pendingChanges = getPendingChanges();
		for (PendingChange<T> change: pendingChanges) {
			BlueKey key = change.getKey();
			List<Segment<T>> segments = collection.getSegmentManager().getAllSegments(key);
			try {
				for (Segment<T> segment: segments) {
					change.applyChange(segment);
				}
				removeChange(change);
			} catch (BlueDbException e) {
				e.printStackTrace();
			}
		}
	}
}
