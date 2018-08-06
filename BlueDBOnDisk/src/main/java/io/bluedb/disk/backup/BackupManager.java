package io.bluedb.disk.backup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.lock.BlueReadLock;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;
import io.bluedb.zip.ZipUtils;

public class BackupManager {
	
	private final Path dbPath;

	public BackupManager(BlueDbOnDisk db) {
		this.dbPath = db.getPath();
	}

	public void backup(List<BlueCollectionOnDisk<?>> collectionsToBackup, Path backupPath) throws BlueDbException, IOException {
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

	public void backupToTempDirectory(List<BlueCollectionOnDisk<?>> collectionsToBackup, Path tempFolder) throws BlueDbException {
		long backupStartTime = System.currentTimeMillis();
		for (BlueCollectionOnDisk<?> collection: collectionsToBackup) {
			copyMetaData(collection, tempFolder);
			copyDataFolders(collection, tempFolder);
		}
		long backupEndTime = System.currentTimeMillis();
		for (BlueCollectionOnDisk<?> collection: collectionsToBackup) {
			copyChanges(collection, backupStartTime, backupEndTime, tempFolder);
		}
	}

	private void copyMetaData(BlueCollectionOnDisk<?> collection, Path tempFolder) throws BlueDbException {
		Path srcPath = collection.getMetaData().getPath();
		Path dstPath = translatePath(dbPath, tempFolder, srcPath);
		FileManager.copyDirectoryWithoutLock(srcPath, dstPath);
	}

	private void copyDataFolders(BlueCollectionOnDisk<?> collection, Path tempFolder) throws BlueDbException {
		for (Segment<?> segment: collection.getSegmentManager().getExistingSegments(Long.MIN_VALUE, Long.MAX_VALUE)) {
			copyDataFolders(segment, tempFolder);
		}
	}

	private void copyDataFolders(Segment<?> segment, Path tempFolder) throws BlueDbException {
		Range range = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		List<File> files = segment.getOrderedFilesInRange(range);
		for (File file: files) {
			Range fileRange = Range.fromUnderscoreDelmimitedString(file.getName());
			long groupingNumber = fileRange.getStart();
			try (BlueReadLock<Path> lock = segment.getReadLockFor(groupingNumber)) {
				Path src = lock.getKey();
				Path dst = translatePath(dbPath, tempFolder, src);
				FileManager.copyFileWithoutLock(src, dst);  // already have read lock on src, shouldn't need write lock on dst
			}
		}
	}

	private void copyChanges(BlueCollectionOnDisk<?> collection, long backupStartTime, long backupEndTime, Path tempFolder) throws BlueDbException {
		RecoveryManager<?> recoveryManager = collection.getRecoveryManager();
		List<File> changesToCopy = recoveryManager.getChangeHistory(backupStartTime, backupEndTime);
		Path historyFolderPath = recoveryManager.getHistoryFolder();
		Path destinationFolderPath = translatePath(dbPath, tempFolder, historyFolderPath);
		destinationFolderPath.toFile().mkdirs();
		for (File file: changesToCopy) {
			Path destinationPath = Paths.get(destinationFolderPath.toString(), file.getName());
			FileManager.copyFileWithoutLock(file.toPath(), destinationPath);
			recoveryManager.markChangePending(destinationPath);
		}
	}

	public static Path translatePath(Path fromPath, Path toPath, Path targetPath) {
		Path relativePath = fromPath.relativize(targetPath);
		return Paths.get(toPath.toString(), relativePath.toString());
	}
}
