package io.bluedb.disk.recovery;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.serialization.BlueSerializer;

public class RecoveryManager<T extends Serializable> {
	
	protected static String RECOVERY_FOLDER = ".recovery";
	protected static String HISTORY_SUBFOLDER = "changes_history";
	protected static String SUFFIX = ".chg";
	protected static String SUFFIX_PENDING = ".pending.chg";
	protected static String SUFFIX_COMPLETE = ".complete.chg";
	private static long FREQUENCY_OF_COMPLETED_CLEANUP = TimeUnit.MINUTES.toMillis(1);
	private static long DEFAULT_RETENTION_PERIOD = TimeUnit.MINUTES.toMillis(1);

	private final BlueCollectionOnDisk<T> collection;
	private final Path recoveryPath;
	private final Path historyFolderPath;
	private final FileManager fileManager;
	private final AtomicLong lastRecoverableId;
	private long lastLogCleanup = 0;
	private long retentionPeriod = DEFAULT_RETENTION_PERIOD;
	private final AtomicInteger holdsOnHistoryCleanup = new AtomicInteger(0);

	public RecoveryManager(BlueCollectionOnDisk<T> collection, FileManager fileManager, BlueSerializer serializer) {
		this.collection = collection;
		this.fileManager = fileManager;
		this.recoveryPath = Paths.get(collection.getPath().toString(), RECOVERY_FOLDER);
		this.historyFolderPath = Paths.get(recoveryPath.toString(), HISTORY_SUBFOLDER);
		lastRecoverableId = new AtomicLong(0);
	}

	public void saveChange(Recoverable<?> change) throws BlueDbException {
		change.setRecoverableId(getNewRecoverableId());
		String filename = getPendingFileName(change);
		Path historyPath = Paths.get(historyFolderPath.toString(), filename);
		fileManager.saveObject(historyPath, change);
		if (isTimeForHistoryCleanup()) {
			cleanupHistory();  // TODO run in a different thread?
		}
	}

	public void setRetentionPeriod(long retentionMillis) {
		this.retentionPeriod = retentionMillis;
	}

	public void placeHoldOnHistoryCleanup() {
		holdsOnHistoryCleanup.incrementAndGet();
	}

	public void removeHoldOnHistoryCleanup() {
		holdsOnHistoryCleanup.decrementAndGet();
	}

	public Path getHistoryFolder() {
		return historyFolderPath;
	}

	public void markComplete(Recoverable<?> change) throws BlueDbException {
		String pendingFileName = getPendingFileName(change);
		String completedFileName = getCompletedFileName(change);
		Path pendingPath = Paths.get(historyFolderPath.toString(), pendingFileName);
		Path completedPath = Paths.get(historyFolderPath.toString(), completedFileName);
		FileManager.moveWithoutLock(pendingPath, completedPath);
	}

	public void markChangePending(Path completedPath) throws BlueDbException {
		String completedPathString = completedPath.toString();
		String pendingPathString = completedPathString.replaceAll(SUFFIX_COMPLETE, SUFFIX_PENDING);
		Path pendingPath = Paths.get(pendingPathString);
		FileManager.moveWithoutLock(completedPath, pendingPath);
	}

	protected boolean isTimeForHistoryCleanup() {
		long timeSinceLastCleanup = System.currentTimeMillis() - lastLogCleanup;
		return timeSinceLastCleanup > FREQUENCY_OF_COMPLETED_CLEANUP;
	}

	public void cleanupHistory() throws BlueDbException {
		if (holdsOnHistoryCleanup.get() > 0) {
			return;
		}
		List<File> historicChangeFiles = FileManager.getFolderContents(historyFolderPath, SUFFIX);
		List<TimeStampedFile> timestampedFiles = Blutils.map(historicChangeFiles, (f) -> new TimeStampedFile(f) );
		long minTimeStamp = System.currentTimeMillis() - retentionPeriod;
		List<TimeStampedFile> filesOldEnoughToDelete = Blutils.filter(timestampedFiles, (f) -> f.getTimestamp() < minTimeStamp);
		filesOldEnoughToDelete.forEach((f) -> f.getFile().delete());
		lastLogCleanup = System.currentTimeMillis();
	}

	public List<File> getChangeHistory(long backupStartTime, long backupEndTime) throws BlueDbException {
		List<File> files = FileManager.getFolderContents(historyFolderPath, SUFFIX);
		if (files.isEmpty()) {
			return files;
		}
		List<TimeStampedFile> timestampedFiles = Blutils.map(files, (f) -> new TimeStampedFile(f) );
		Collections.sort(timestampedFiles);
		// Note: the last change before backup might by incomplete so we'll include it
		int lastChangeBeforeBackup = Blutils.lastIndex(timestampedFiles, (t) -> t.getTimestamp() < backupStartTime );
		int firstChangeToKeep = Math.max(0,  lastChangeBeforeBackup);
		int lastChangeToKeep = Blutils.lastIndex(timestampedFiles, (t) -> t.getTimestamp() <= backupEndTime );
		List<TimeStampedFile> relevantTimeStampedFiles = timestampedFiles.subList(firstChangeToKeep, lastChangeToKeep + 1);
		return Blutils.map(relevantTimeStampedFiles, (tsf) -> tsf.getFile() );
	}

	public List<Recoverable<T>> getPendingChanges() throws BlueDbException {
		List<File> pendingChangeFiles = FileManager.getFolderContents(historyFolderPath, SUFFIX_PENDING);
		List<Recoverable<T>> changes = new ArrayList<>();
		for (File file: pendingChangeFiles) {
			@SuppressWarnings("unchecked")
			Recoverable<T> change = (Recoverable<T>) fileManager.loadObject(file.toPath());
			changes.add(change);
		}
		Collections.sort(changes);
		return changes;
	}

	public void recover() throws BlueDbException {
		for (Recoverable<T> change: getPendingChanges()) {
			change.apply(collection);
			markComplete(change);
		}
	}

	public long getNewRecoverableId() {
		return lastRecoverableId.getAndIncrement();
	}

	public static String getCompletedFileName(Recoverable<?> change) {
		return  String.valueOf(change.getTimeCreated()) + "." + String.valueOf(change.getRecoverableId()) + SUFFIX_COMPLETE;
	}

	public static String getPendingFileName(Recoverable<?> change) {
		return  String.valueOf(change.getTimeCreated()) + "." + String.valueOf(change.getRecoverableId()) + SUFFIX_PENDING;
	}
}
