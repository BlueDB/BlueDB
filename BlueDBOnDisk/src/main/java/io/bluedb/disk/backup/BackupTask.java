package io.bluedb.disk.backup;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.BlueDbOnDisk;
import io.bluedb.disk.collection.BlueCollectionOnDisk;
import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.lock.BlueReadLock;
import io.bluedb.disk.recovery.RecoveryManager;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;

public class BackupTask {
	
	private final BlueDbOnDisk db;
	private final Path backupPath;
	
	public BackupTask(BlueDbOnDisk db, Path backupPath) {
		this.db = db;
		this.backupPath = backupPath;
	}

	public void backup(List<BlueCollectionOnDisk<?>> collectionsToBackup) throws BlueDbException {
		long backupStartTime = System.currentTimeMillis();
		for (BlueCollectionOnDisk<?> collection: collectionsToBackup) {
			copyMetaData(collection, backupPath);
			copyDataFolders(collection, backupPath);
		}
		long backupEndTime = System.currentTimeMillis();
		for (BlueCollectionOnDisk<?> collection: collectionsToBackup) {
			copyChanges(collection, backupStartTime, backupEndTime);
		}
	}

	private void copyMetaData(BlueCollectionOnDisk<?> collection, Path backupPath2) throws BlueDbException {
		Path srcPath = collection.getMetaData().getPath();
		Path dstPath = translatePath(srcPath);
		FileManager.copyDirectoryWithoutLock(srcPath, dstPath);
	}

	private void copyDataFolders(BlueCollectionOnDisk<?> collection, Path backupPath2) throws BlueDbException {
		for (Segment<?> segment: collection.getSegmentManager().getExistingSegments(Long.MIN_VALUE, Long.MAX_VALUE)) {
			copyDataFolders(segment);
		}
	}

	private void copyDataFolders(Segment<?> segment) throws BlueDbException {
		Range range = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		List<File> files = segment.getOrderedFilesInRange(range);
		for (File file: files) {
			Range fileRange = Range.fromUnderscoreDelmimitedString(file.getName());
			long groupingNumber = fileRange.getStart();
			try (BlueReadLock<Path> lock = segment.getReadLockFor(groupingNumber)) {
				Path src = lock.getKey();
				Path dst = translatePath(src);
				FileManager.copyFileWithoutLock(src, dst);  // TODO use copy with lock?
			}
		}
	}

	private void copyChanges(BlueCollectionOnDisk<?> collection, long backupStartTime, long backupEndTime) throws BlueDbException {
		RecoveryManager<?> recoveryManager = collection.getRecoveryManager();
		List<File> changesToCopy = recoveryManager.getChangeHistory(backupStartTime, backupEndTime);
		Path pendingFolderPath = recoveryManager.getPendingFolderPath();
		Path destinationFolderPath = translatePath(pendingFolderPath);
		destinationFolderPath.toFile().mkdirs();
		for (File file: changesToCopy) {
			Path destinationPath = Paths.get(destinationFolderPath.toString(), file.getName());
			FileManager.copyFileWithoutLock(file.toPath(), destinationPath);
		}
	}

	public Path translatePath(Path targetPath) {
		Path dbPath = db.getPath();
		Path relativePath = dbPath.relativize(targetPath);
		return Paths.get(backupPath.toString(), relativePath.toString());
	}
}
