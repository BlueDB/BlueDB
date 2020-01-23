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
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadWriteSegment;
import org.bluedb.zip.ZipUtils;

public class BackupManager {
	
	private final Path dbPath;

	public BackupManager(ReadableDbOnDisk db) {
		this.dbPath = db.getPath();
	}

	public void backup(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path backupPath) throws BlueDbException, IOException {
		try {
			collectionsToBackup.forEach((c) -> (c).getRecoveryManager().placeHoldOnHistoryCleanup());
			Path tempDirectoryPath = Files.createTempDirectory("bluedb_backup_in_progress");
			tempDirectoryPath.toFile().deleteOnExit();
			Path unzippedBackupPath = Paths.get(tempDirectoryPath.toString(), "bluedb");
			backupToTempDirectory(collectionsToBackup, unzippedBackupPath);
			ZipUtils.zipFile(unzippedBackupPath, backupPath);
			Blutils.recursiveDelete(tempDirectoryPath.toFile());
		} finally {
			collectionsToBackup.forEach((c) -> (c).getRecoveryManager().removeHoldOnHistoryCleanup());
		}
	}

	public void backupToTempDirectory(List<ReadWriteCollectionOnDisk<?>> collectionsToBackup, Path tempFolder) throws BlueDbException {
		long backupStartTime = System.currentTimeMillis();
		for (ReadWriteCollectionOnDisk<?> collection: collectionsToBackup) {
			copyMetaData(collection, tempFolder);
			copyDataFolders(collection, tempFolder);
		}
		long backupEndTime = System.currentTimeMillis();
		for (ReadWriteCollectionOnDisk<?> collection: collectionsToBackup) {
			copyChanges(collection, backupStartTime, backupEndTime, tempFolder);
		}
	}

	private void copyMetaData(ReadWriteCollectionOnDisk<?> collection, Path tempFolder) throws BlueDbException {
		Path srcPath = collection.getMetaData().getPath();
		Path dstPath = translatePath(dbPath, tempFolder, srcPath);
		FileUtils.copyDirectoryWithoutLock(srcPath, dstPath);
	}

	private void copyDataFolders(ReadWriteCollectionOnDisk<?> collection, Path tempFolder) throws BlueDbException {
		for (ReadWriteSegment<?> segment: collection.getSegmentManager().getAllExistingSegments()) {
			copyDataFolders(segment, tempFolder);
		}
	}

	private void copyDataFolders(ReadWriteSegment<?> segment, Path tempFolder) throws BlueDbException {
		Range range = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		List<File> files = segment.getOrderedFilesInRange(range);
		for (File file: files) {
			Range fileRange = Range.fromUnderscoreDelmimitedString(file.getName());
			long groupingNumber = fileRange.getStart();
			try (BlueReadLock<Path> lock = segment.getReadLockFor(groupingNumber)) {
				Path src = lock.getKey();
				Path dst = translatePath(dbPath, tempFolder, src);
				FileUtils.copyFileWithoutLock(src, dst);  // already have read lock on src, shouldn't need write lock on dst
			}
		}
	}

	private void copyChanges(ReadWriteCollectionOnDisk<?> collection, long backupStartTime, long backupEndTime, Path tempFolder) throws BlueDbException {
		RecoveryManager<?> recoveryManager = collection.getRecoveryManager();
		List<File> changesToCopy = recoveryManager.getChangeHistory(backupStartTime, backupEndTime);
		Path historyFolderPath = recoveryManager.getHistoryFolder();
		Path destinationFolderPath = translatePath(dbPath, tempFolder, historyFolderPath);
		destinationFolderPath.toFile().mkdirs();
		for (File file: changesToCopy) {
			Path destinationPath = Paths.get(destinationFolderPath.toString(), file.getName());
			FileUtils.copyFileWithoutLock(file.toPath(), destinationPath);
			recoveryManager.markChangePending(destinationPath);
		}
	}

	public static Path translatePath(Path fromPath, Path toPath, Path targetPath) {
		Path relativePath = fromPath.relativize(targetPath);
		return Paths.get(toPath.toString(), relativePath.toString());
	}
}
