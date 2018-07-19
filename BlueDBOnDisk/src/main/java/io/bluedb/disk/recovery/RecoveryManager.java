package io.bluedb.disk.recovery;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class RecoveryManager<T extends Serializable> {
	
	protected static String RECOVERY_FOLDER = ".recovery";
	protected static String PENDING_SUBFOLDER = "changes_pending";
	protected static String HISTORY_SUBFOLDER = "changes_history";
	protected static String SUFFIX = ".chg";
	private static long FREQUENCY_OF_COMPLETED_CLEANUP = 10 * 60 * 1000; // ten minutes
	private static long COMPLETED_RETENTION = 60 * 60 * 1000; // one hour

	private final BlueCollectionOnDisk<T> collection;
	private final Path recoveryPath;
	private final Path pendingFolderPath;
	private final Path historyFolderPath;
	private final FileManager fileManager;
	private long lastLogCleanup = 0;
	
	public RecoveryManager(BlueCollectionOnDisk<T> collection, FileManager fileManager, BlueSerializer serializer) {
		this.collection = collection;
		this.fileManager = fileManager;
		this.recoveryPath = Paths.get(collection.getPath().toString(), RECOVERY_FOLDER);
		this.pendingFolderPath = Paths.get(recoveryPath.toString(), PENDING_SUBFOLDER);
		this.historyFolderPath = Paths.get(recoveryPath.toString(), HISTORY_SUBFOLDER);
	}

	public void saveChange(Recoverable<?> change) throws BlueDbException {
		String filename = getFileName(change);
		Path pendingPath = Paths.get(pendingFolderPath.toString(), filename);
		Path historyPath = Paths.get(historyFolderPath.toString(), filename);
		fileManager.saveObject(pendingPath, change);
		fileManager.saveObject(historyPath, change);
	}

	public Path getPendingFolderPath() {
		return pendingFolderPath;
	}

	public void removeChange(Recoverable<?> change) throws BlueDbException {
		String filename = getFileName(change);
		Path path = Paths.get(pendingFolderPath.toString(), filename);
		File file = new File(path.toString());
		file.delete();
		if (isTimeForHistoryCleanup()) {
			cleanupHistory();  // TODO run in a different thread?
		}
	}

	protected boolean isTimeForHistoryCleanup() {
		long timeSinceLastCleanup = System.currentTimeMillis() - lastLogCleanup;
		return timeSinceLastCleanup > FREQUENCY_OF_COMPLETED_CLEANUP;
	}

	protected void cleanupHistory() {
		// TODO check if restore is pending
		List<File> historicChangeFiles = FileManager.getFolderContents(historyFolderPath, SUFFIX);
		List<File> filesOldEnoughToDelete = Blutils.filter(historicChangeFiles, (f) -> isChangeFileOlderThan(f, COMPLETED_RETENTION));
		filesOldEnoughToDelete.forEach((File f) -> f.delete());
		lastLogCleanup = System.currentTimeMillis();
	}

	public List<File> getChangeHistory(long backupStartTime, long backupEndTime) throws BlueDbException {
		List<File> files = FileManager.getFolderContents(historyFolderPath, SUFFIX);
		if (files.isEmpty()) {
			return files;
		}
		Blutils.sortByMappedValue(files, (File f) -> extractTimestamp(f) );

		int firstChangeToKeep = 0;
		int lastChangeToKeep = -1;
		
		for (int i=0; i<files.size(); i++) {
			File file = files.get(i);
			long timestamp = extractTimestamp(file);
			if (timestamp < backupStartTime) {
				firstChangeToKeep = i;
			}
			if (timestamp <= backupEndTime) {
				lastChangeToKeep = i;
			}
		}
		
		
		return files.subList(firstChangeToKeep, lastChangeToKeep + 1);
	}

	public List<Recoverable<T>> getPendingChanges() {
		List<File> pendingChangeFiles = FileManager.getFolderContents(pendingFolderPath, SUFFIX);
		List<Recoverable<T>> changes = new ArrayList<>();
		for (File file: pendingChangeFiles) {
			try {
				@SuppressWarnings("unchecked")
				Recoverable<T> change = (Recoverable<T>) fileManager.loadObject(file.toPath());
				changes.add(change);
			} catch (Throwable t) {
				t.printStackTrace();
				// TODO handle broken files, ignoring for now
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

	public static Long extractTimestamp(File file) {
		try {
			String fileName = file.getName();
			String longString = fileName.split("[.]")[0];
			return Long.valueOf(longString);
		} catch (Throwable t) {
			return null;
		}
	}

	public static String getFileName(Recoverable<?> change) {
		return  String.valueOf(change.getTimeCreated()) + SUFFIX;
	}

	protected static boolean isChangeFileOlderThan(File file, long age) {
		Long timestamp = extractTimestamp(file);
		long minTimestamp = System.currentTimeMillis() - age;
		return timestamp != null && timestamp < minTimestamp;
	}
}
