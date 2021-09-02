package org.bluedb.disk.recovery;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bluedb.disk.Blutils;

public class ChangeHistoryCleaner {

	private static int DEFAULT_RETENTION_LIMIT = 200;
	private static int DEFAULT_BATCH_REMOVAL_LIMIT = 10000;
	private int completedChangeLimit = DEFAULT_RETENTION_LIMIT;
	private int completedChangeBatchRemovalLimit = DEFAULT_BATCH_REMOVAL_LIMIT;
	private final AtomicInteger holdsOnHistoryCleanup = new AtomicInteger(0);
	private long waitBetweenCleanups = 5_000;
	final Path historyFolderPath;
	RecoveryManager<?> recoveryManager;

	public ChangeHistoryCleaner(RecoveryManager<?> recoveryManager) {
		this.recoveryManager = recoveryManager;
		this.historyFolderPath = recoveryManager.getHistoryFolder();
		
		Runnable cleanupTask = Blutils.surroundTaskWithTryCatch(this::cleanupHistory);
		recoveryManager.getCollection().getSharedExecutor().scheduleTaskAtFixedRate(cleanupTask, waitBetweenCleanups, waitBetweenCleanups, TimeUnit.MILLISECONDS);
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
		try (DirectoryStream<Path> completedChangeFileStream = recoveryManager.getCompletedChangeFilesAsStream()){
			Iterator<Path> iterator = completedChangeFileStream.iterator();
			List<TimeStampedFile> timestampedFiles = new LinkedList<TimeStampedFile>();
			while (iterator.hasNext()) {
				timestampedFiles.addAll(getNextBatchOfTimestampedFiles(iterator));
				Collections.sort(timestampedFiles);
				
				int numFilesToDelete = Math.max(0, timestampedFiles.size() - completedChangeLimit);
				List<TimeStampedFile> filesToDelete = timestampedFiles.subList(0, numFilesToDelete);
				timestampedFiles.removeAll(filesToDelete);

				filesToDelete.forEach((f) -> f.getFile().delete());
			}
		} catch (IOException ex) {
			
		}
	}

	private Collection<TimeStampedFile> getNextBatchOfTimestampedFiles(Iterator<Path> iterator) {
		List<File> historicChangeFiles = new LinkedList<>();
		while (iterator.hasNext() && historicChangeFiles.size() < completedChangeBatchRemovalLimit) {
			Path next = iterator.next();
			historicChangeFiles.add(next.toFile());
		}
		List<TimeStampedFile> timestampedFiles = Blutils.mapIgnoringExceptions(historicChangeFiles, (f) -> new TimeStampedFile(f) );
		return timestampedFiles;
	}

	public void setWaitBetweenCleanups(long millis) {
		this.waitBetweenCleanups = millis;
	}
}
