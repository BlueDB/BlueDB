package org.bluedb.disk.backup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.recovery.PendingBatchChange;
import org.bluedb.disk.recovery.PendingChange;
import org.bluedb.disk.recovery.Recoverable;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.zip.ZipUtils;

public class BackupManager {
	
	private final Path dbPath;

	public BackupManager(ReadableDbOnDisk db) {
		this.dbPath = db.getPath();
	}
	
	public void backup(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path backupPath) throws BlueDbException, IOException {
		backup(collectionsToBackup, backupPath, Range.createMaxRange());
	}
	
	public void backup(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path backupPath, Range includedTimeRange) throws BlueDbException, IOException {
		backup(collectionsToBackup, backupPath, includedTimeRange, false);
	}
	
	public void backup(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path backupPath, Range includedTimeRange, boolean acceptAllPendingChanges) throws BlueDbException, IOException {
		try {
			collectionsToBackup.forEach((c) -> (c).getRecoveryManager().placeHoldOnHistoryCleanup());
			Path tempDirectoryPath = Files.createTempDirectory("bluedb_backup_in_progress");
			tempDirectoryPath.toFile().deleteOnExit();
			Path unzippedBackupPath = Paths.get(tempDirectoryPath.toString(), "bluedb");
			backupToTempDirectory(collectionsToBackup, unzippedBackupPath, includedTimeRange, acceptAllPendingChanges);
			ZipUtils.zipFile(unzippedBackupPath, backupPath);
			Blutils.recursiveDelete(tempDirectoryPath.toFile());
		} finally {
			collectionsToBackup.forEach((c) -> (c).getRecoveryManager().removeHoldOnHistoryCleanup());
		}
	}
	
	protected void backupToTempDirectory(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path tempFolder, Range includedTimeRange, boolean acceptAllPendingChanges) throws BlueDbException {
		Range dataBackupRuntime = backupDataToTempDirectory(collectionsToBackup, tempFolder, includedTimeRange);
		Range includedChangeTimes = acceptAllPendingChanges ? Range.createMaxRange() : dataBackupRuntime;
		backupNewChangesToTempDirectory(collectionsToBackup, tempFolder, includedTimeRange, includedChangeTimes);
	}

	protected Range backupDataToTempDirectory(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path tempFolder, Range includedTimeRange) throws BlueDbException {
		long dataBackupStartTime = System.currentTimeMillis();
		for (ReadWriteCollectionOnDisk<?> collection: collectionsToBackup) {
			copyMetaData(collection, tempFolder);
			copyDataFolders(collection, tempFolder, includedTimeRange);
		}
		long dataBackupEndTime = System.currentTimeMillis();
		return new Range(dataBackupStartTime, dataBackupEndTime);
	}

	protected void backupNewChangesToTempDirectory(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path tempFolder, Range includedTimeRange, Range includedChangeTimes) throws BlueDbException {
		for (ReadWriteCollectionOnDisk<?> collection: collectionsToBackup) {
			copyChanges(collection, includedTimeRange, includedChangeTimes, tempFolder);
		}
	}

	private void copyMetaData(ReadWriteCollectionOnDisk<?> collection, Path tempFolder) throws BlueDbException {
		Path srcPath = collection.getMetaData().getPath();
		Path dstPath = translatePath(dbPath, tempFolder, srcPath);
		FileUtils.copyDirectoryWithoutLock(srcPath, dstPath);
	}

	private void copyDataFolders(ReadWriteCollectionOnDisk<?> collection, Path tempFolder, Range includedTimeRange) throws BlueDbException {
		Range fileRangeToConsider = createFileRangeToConsider(includedTimeRange, collection.isTimeBased());
		
		for (ReadWriteSegment<?> segment: collection.getSegmentManager().getAllExistingSegments()) {
			copyDataFolders(segment, tempFolder, includedTimeRange, collection.isTimeBased(), fileRangeToConsider);
		}
	} 

	private Range createFileRangeToConsider(Range includedTimeRange, boolean isTimeBased) {
		if(!isTimeBased) {
			return Range.createMaxRange(); //Include everything in a non time base collection
		}
	
		/*
		 * Since values with a TimeFrameKey have an end time, we have to look at anything with a start time at or
		 * before the end time of the included time range since any of those objects could be long enough to overlap
		 * into the included time range. And since you can insert a value with a TimeFrameKey into a Collection with
		 * a key type of TimeKey we must do this even for collections with a key type of TimeKey. 
		 */
		return new Range(Long.MIN_VALUE, includedTimeRange.getEnd());
	}

	private void copyDataFolders(ReadWriteSegment<?> segment, Path tempFolder, Range includedTimeRange, boolean isTimeBased, Range fileRangeToConsider) throws BlueDbException {
		List<File> files = segment.getOrderedFilesInRange(fileRangeToConsider);
		for (File file: files) {
			Range fileRange = Range.fromUnderscoreDelmimitedString(file.getName());
			long groupingNumber = fileRange.getStart();
			
			if(isTimeBased && !includedTimeRange.isMaxRange()) {
				copyDataFileAfterFilteringBasedOnTime(segment, tempFolder, includedTimeRange, groupingNumber);
			} else {
				copyDataFileStraightOver(segment, tempFolder, groupingNumber);
			}
		}
	}

	private void copyDataFileAfterFilteringBasedOnTime(ReadWriteSegment<?> segment, Path tempFolder, Range includedTimeRange, long groupingNumber) throws BlueDbException {
		Path src = segment.getPathFor(groupingNumber);
		Path dst = translatePath(dbPath, tempFolder, src);
		FileUtils.ensureDirectoryExists(dst.toFile());
		boolean isFileEmpty = true;
		
		try(BlueObjectOutput<BlueEntity<?>> output = BlueObjectOutput.createWithoutLockOrSerializer(dst)) {
			try(BlueObjectInput<?> fileInputStream = segment.getObjectInputFor(groupingNumber)) {
				while(fileInputStream.hasNext()) {
					BlueEntity<?> next = (BlueEntity<?>) fileInputStream.next();
					if(next.getKey().isInRange(includedTimeRange.getStart(), includedTimeRange.getEnd())) {
						output.writeBytes(fileInputStream.getLastBytes());
						isFileEmpty = false;
					}
				}
			}
		}
		
		if(isFileEmpty) {
			FileUtils.deleteIfExistsWithoutLock(dst);
		}
	}

	private void copyDataFileStraightOver(ReadWriteSegment<?> segment, Path tempFolder, long groupingNumber) throws BlueDbException {
		try (BlueReadLock<Path> lock = segment.getReadLockFor(groupingNumber)) {
			Path src = lock.getKey();
			Path dst = translatePath(dbPath, tempFolder, src);
			FileUtils.copyFileWithoutLock(src, dst);  // already have read lock on src, shouldn't need write lock on dst
		}
	}

	private void copyChanges(ReadWriteCollectionOnDisk<?> collection, Range includedTimeRange, Range includedChangeTimes, Path tempFolder) throws BlueDbException {
		RecoveryManager<?> recoveryManager = collection.getRecoveryManager();
		List<File> changesToCopy = recoveryManager.getChangeHistory(includedChangeTimes.getStart(), includedChangeTimes.getEnd());
		Path historyFolderPath = recoveryManager.getHistoryFolder();
		Path destinationFolderPath = translatePath(dbPath, tempFolder, historyFolderPath);
		destinationFolderPath.toFile().mkdirs();
		for (File file: changesToCopy) {
			Path destinationPath = Paths.get(destinationFolderPath.toString(), file.getName());
			if(!includedTimeRange.isMaxRange() && collection.isTimeBased()) {
				copyChangeAfterFilteringBasedOnTime(collection, includedTimeRange, recoveryManager, file, destinationPath);
			} else {
				copyChangeStraightOver(recoveryManager, file, destinationPath);
			}
		}
	}

	private void copyChangeAfterFilteringBasedOnTime(ReadWriteCollectionOnDisk<?> collection, Range includedTimeRange, RecoveryManager<?> recoveryManager, File file, Path destinationPath) throws BlueDbException {
		Recoverable<?> change = (Recoverable<?>) collection.getFileManager().loadObject(file);
		if(shouldCopyChange(includedTimeRange, change)) {
			collection.getFileManager().saveObject(destinationPath, change);
			recoveryManager.markChangePending(destinationPath);
		}
	}

	private boolean shouldCopyChange(Range includedTimeRange, Recoverable<?> change) {
		if(change instanceof PendingChange) {
			if(((PendingChange<?>)change).getKey().isInRange(includedTimeRange.getStart(), includedTimeRange.getEnd())) {
				return true;
			} else {
				return false;
			}
		}
		
		if(change instanceof PendingBatchChange) {
			PendingBatchChange<?> batchChange = (PendingBatchChange<?>) change;
			batchChange.removeChangesOutsideRange(includedTimeRange);
			if(!batchChange.isEmpty()) {
				return true;
			} else {
				return false;
			}
		}
		
		return true; //We don't need to check rollups
	}

	private void copyChangeStraightOver(RecoveryManager<?> recoveryManager, File file, Path destinationPath) throws BlueDbException {
		FileUtils.copyFileWithoutLock(file.toPath(), destinationPath);
		recoveryManager.markChangePending(destinationPath);
	}

	public static Path translatePath(Path fromPath, Path toPath, Path targetPath) {
		Path relativePath = fromPath.relativize(targetPath);
		return Paths.get(toPath.toString(), relativePath.toString());
	}
}
