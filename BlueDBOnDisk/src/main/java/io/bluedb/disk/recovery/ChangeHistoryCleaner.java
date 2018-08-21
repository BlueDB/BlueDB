package io.bluedb.disk.recovery;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import io.bluedb.disk.Blutils;

public class ChangeHistoryCleaner implements Runnable {

	private static int DEFAULT_RETENTION_LIMIT = 200;
	private static int DEFAULT_NUMBER_CHANGES_BETWEEN_CLEANUPS = 100;
	private int completedChangeLimit = DEFAULT_RETENTION_LIMIT;
	private final AtomicInteger holdsOnHistoryCleanup = new AtomicInteger(0);
	private AtomicInteger changesSinceLastCleanup = new AtomicInteger(0);
	private boolean isStopped = false;
	final Path historyFolderPath;
	RecoveryManager<?> recoveryManager;
	Thread thread;

	public ChangeHistoryCleaner(RecoveryManager<?> recoveryManager) {
		this.recoveryManager = recoveryManager;
		this.historyFolderPath = recoveryManager.getHistoryFolder();
		thread = new Thread(this);
		thread.start();
	}

	public void reportChange() {
		changesSinceLastCleanup.incrementAndGet();
	}

	public boolean isTimeForHistoryCleanup() {	
		return changesSinceLastCleanup.get() > DEFAULT_NUMBER_CHANGES_BETWEEN_CLEANUPS;	
	}

	public void setRetentionLimit(int completedChangeLimit) {
		this.completedChangeLimit = completedChangeLimit;
	}

	public void placeHoldOnHistoryCleanup() {
		holdsOnHistoryCleanup.incrementAndGet();
	}

	public void removeHoldOnHistoryCleanup() {
		holdsOnHistoryCleanup.decrementAndGet();
	}

	public List<TimeStampedFile> getCompletedTimeStampedFiles() {
		List<File> historicChangeFiles = recoveryManager.getCompletedChangeFiles();
		List<TimeStampedFile> timestampedFiles = Blutils.mapIgnoringExceptions(historicChangeFiles, (f) -> new TimeStampedFile(f) );
		return timestampedFiles;
	}

	public void cleanupHistory() {
		if (holdsOnHistoryCleanup.get() > 0) {
			return;
		}
		changesSinceLastCleanup.set(0);
		List<TimeStampedFile> timestampedFiles = getCompletedTimeStampedFiles();
		Collections.sort(timestampedFiles);
		int numFilesToDelete = Math.max(0, timestampedFiles.size() - completedChangeLimit);
		List<TimeStampedFile> filesToDelete = timestampedFiles.subList(0, numFilesToDelete);
		filesToDelete.forEach((f) -> f.getFile().delete());
	}

	public void stop() {
		isStopped = true;
	}

	@Override
	public void run() {
		while(!isStopped) {
			while (changesSinceLastCleanup.get() < DEFAULT_NUMBER_CHANGES_BETWEEN_CLEANUPS) {
				Blutils.trySleep(500);
			}
			cleanupHistory();
		}
	}
}
