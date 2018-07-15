package io.bluedb.disk.recovery;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class RecoveryManager<T extends Serializable> {
	
	protected static String SUBFOLDER = ".pending";
	protected static String SUFFIX = ".chg";

	private final BlueCollectionOnDisk<T> collection;
	private final Path recoveryPath;
	private final FileManager fileManager;
	
	public RecoveryManager(BlueCollectionOnDisk<T> collection, FileManager fileManager, BlueSerializer serializer) {
		this.collection = collection;
		this.fileManager = fileManager;
		this.recoveryPath = Paths.get(collection.getPath().toString(), SUBFOLDER);
	}

	public void saveChange(Recoverable<?> change) throws BlueDbException {
		String filename = getFileName(change);
		Path path = Paths.get(recoveryPath.toString(), filename);
		fileManager.saveObject(path, change);
	}

	public void removeChange(Recoverable<?> change) throws BlueDbException {
		String filename = getFileName(change);
		Path path = Paths.get(recoveryPath.toString(), filename);
		File file = new File(path.toString());
		file.delete();
	}

	public static String getFileName(Recoverable<?> change) {
		return  String.valueOf(change.getTimeCreated()) + SUFFIX;
	}

	public List<Recoverable<T>> getPendingChanges() {
		List<File> pendingChangeFiles = FileManager.getFolderContents(recoveryPath, SUFFIX);
		List<Recoverable<T>> changes = new ArrayList<>();
		for (File file: pendingChangeFiles) {
			try {
				@SuppressWarnings("unchecked")
				Recoverable<T> change = (Recoverable<T>) fileManager.loadObject(file.toPath());
				changes.add(change);
			} catch (Throwable t) {
				t.printStackTrace();
				// ignore broken files for now
			}
		}
		return changes;
	}


	public void recover() {
		for (Recoverable<T> change: getPendingChanges()) {
			try {
				change.apply(collection);
				removeChange(change);
			} catch (BlueDbException e) {
				e.printStackTrace();
			}
		}
	}
}
