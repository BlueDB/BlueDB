package org.bluedb.disk.recovery;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.file.BlueObjectStreamSorter;
import org.bluedb.disk.file.BlueObjectStreamSorter.BlueObjectStreamSorterConfig;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.query.QueryOnDisk;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.BlueSerializer;

public class RecoveryManager<T extends Serializable> {
	
	protected static String RECOVERY_FOLDER = ".recovery";
	protected static String HISTORY_SUBFOLDER = "changes_history";
	protected static String SUFFIX = ".chg";
	protected static String SUFFIX_PENDING = ".pending.chg";
	protected static String SUFFIX_COMPLETE = ".complete.chg";

	private final ReadWriteCollectionOnDisk<T> collection;
	private final Path recoveryPath;
	private final Path historyFolderPath;
	private final ReadWriteFileManager fileManager;
	private final AtomicLong lastRecoverableId;
	private final ChangeHistoryCleaner cleaner;

	public RecoveryManager(ReadWriteCollectionOnDisk<T> collection, ReadWriteFileManager fileManager, BlueSerializer serializer) {
		this.collection = collection;
		this.fileManager = fileManager;
		this.recoveryPath = Paths.get(collection.getPath().toString(), RECOVERY_FOLDER);
		this.historyFolderPath = Paths.get(recoveryPath.toString(), HISTORY_SUBFOLDER);
		lastRecoverableId = new AtomicLong(0);
		cleaner = new ChangeHistoryCleaner(this);
	}

	public void saveNewChange(Recoverable<?> change) throws BlueDbException {
		change.setRecoverableId(getNewRecoverableId());
		String filename = getPendingFileName(change);
		Path historyPath = Paths.get(historyFolderPath.toString(), filename);
		fileManager.saveObject(historyPath, change);
	}
	
	@SuppressWarnings("unchecked")
	public void copyChange(Recoverable<?> change, Path destination, Range includedTimeRange) throws BlueDbException {
		if(change instanceof PendingMassChange<?>) {
			PendingMassChange<?> massChange = (PendingMassChange<?>) change;
			try(SortedChangeSupplier<BlueKey> onDiskSortedChangeSupplier = new OnDiskSortedChangeSupplier<>(massChange.getChangesFilePath(), fileManager);
					BlueObjectOutput<IndividualChange<T>> output = fileManager.getBlueOutputStreamWithoutLock(destination)) {
				
				output.setMetadataValue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, String.valueOf(true));
				
				SortedChangeIterator<?> sortedChangeIterator = new SortedChangeIterator<>(onDiskSortedChangeSupplier);
				while(sortedChangeIterator.hasNext()) {
					IndividualChange<?> nextChange = sortedChangeIterator.next();
					if(nextChange.getKey().overlapsRange(includedTimeRange.getStart(), includedTimeRange.getEnd())) {
						output.write((IndividualChange<T>) nextChange);
					}
				}
			}
		} else if(change instanceof PendingBatchChange) {
				PendingBatchChange<?> batchChange = (PendingBatchChange<?>) change;
				batchChange.removeChangesOutsideRange(includedTimeRange);
				if(!batchChange.isEmpty()) {
					fileManager.saveObject(destination, change);
				}
		} else {
			fileManager.saveObject(destination, change);
		}
	}

	public PendingMassChange<T> saveMassChangeForQueryChange(QueryOnDisk<T> updateQuery, EntityToChangeMapper<T> entityToChangeMapper) throws BlueDbException {
		long creationTime = System.currentTimeMillis();
		long recoverableId = getNewRecoverableId();
		Path path = historyFolderPath.resolve(getPendingFileName(creationTime, recoverableId));
		Path tmpPath = FileUtils.createTempFilePath(path);
		tmpPath.getParent().toFile().mkdirs();
		
		try(
				CloseableIterator<BlueEntity<T>> entitiesToUpdateIterator = updateQuery.getEntityIterator();
				BlueObjectOutput<IndividualChange<T>> output = fileManager.getBlueOutputStreamWithoutLock(tmpPath)) {
			
			output.setMetadataValue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, Boolean.toString(true));
			
			while(entitiesToUpdateIterator.hasNext()) {
				BlueEntity<T> entity = entitiesToUpdateIterator.next();
				try {
					IndividualChange<T> change = entityToChangeMapper.map(entity);
					output.write(change);
				} catch(Throwable t) {
					throw new BlueDbException("Update query failed, the update/replace method probably did something weird to the " + entity.getKey() + " entity", t);
				}
			}
		} catch(Throwable t) {
			//If there is a problem don't leave the tmp file around but let the exception still be thrown
			tmpPath.toFile().delete(); 
			throw t;
		}
		
		try {
			FileUtils.moveWithoutLock(tmpPath, path);
			return new PendingMassChange<>(creationTime, recoverableId, path);
		} finally {
			tmpPath.toFile().delete();
		}
	}

	public PendingMassChange<T> saveMassChangeForBatchUpsert(Iterator<IndividualChange<T>> sortedChangeIterator) throws BlueDbException {
		long creationTime = System.currentTimeMillis();
		long recoverableId = getNewRecoverableId();
		Path path = historyFolderPath.resolve(getPendingFileName(creationTime, recoverableId));
		Path tmpPath = FileUtils.createTempFilePath(path);
		tmpPath.getParent().toFile().mkdirs();
		
		try(BlueObjectOutput<IndividualChange<T>> output = fileManager.getBlueOutputStreamWithoutLock(tmpPath)) {
			
			output.setMetadataValue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, Boolean.toString(true));
			
			while(sortedChangeIterator.hasNext()) {
				IndividualChange<T> nextChange = sortedChangeIterator.next();
				try {
					output.write(nextChange);
				} catch(Throwable t) {
					new BlueDbException("Batch upsert failed on object " + (nextChange != null ? nextChange.getKey() : "null") + ". It will be skipped.", t).printStackTrace();
				}
			}
		} catch(Throwable t) {
			//If there is a problem don't leave the tmp file around but let the exception still be thrown
			tmpPath.toFile().delete(); 
			throw t;
		}
		
		try {
			FileUtils.moveWithoutLock(tmpPath, path);
			return new PendingMassChange<>(creationTime, recoverableId, path);
		} finally {
			tmpPath.toFile().delete();
		}
	}
	
	public PendingMassChange<T> saveMassChangeForUnorderedChanges(Iterator<IndividualChange<T>> changeIterator) throws BlueDbException {
		return saveMassChangeForUnorderedChanges(changeIterator, false);
	}
	
	public PendingMassChange<T> saveTempMassChangeForUnorderedChanges(Iterator<IndividualChange<T>> changeIterator) throws BlueDbException {
		return saveMassChangeForUnorderedChanges(changeIterator, true);
	}

	private PendingMassChange<T> saveMassChangeForUnorderedChanges(Iterator<IndividualChange<T>> changeIterator, boolean tempFile) throws BlueDbException {
		long creationTime = System.currentTimeMillis();
		long recoverableId = getNewRecoverableId();
		Path path = historyFolderPath.resolve(getPendingFileName(creationTime, recoverableId));
		if(tempFile) {
			path = FileUtils.createTempFilePath(path);
		}
		Path tmpPath = FileUtils.createTempFilePath(path);
		tmpPath.getParent().toFile().mkdirs();
		
		Map<BlueFileMetadataKey, String> metadataEntries = new HashMap<>();
		metadataEntries.put(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE, String.valueOf(true));
		
		try {
			BlueObjectStreamSorter<IndividualChange<T>> sorter = new BlueObjectStreamSorter<>(changeIterator, tmpPath, fileManager, metadataEntries, BlueObjectStreamSorterConfig.createDefault());
			sorter.sortAndWriteToFile();
		
			FileUtils.moveWithoutLock(tmpPath, path);
			return new PendingMassChange<>(creationTime, recoverableId, path);
		} finally {
			tmpPath.toFile().delete();
		}
	}
	
	public Path getHistoryFolder() {
		return historyFolderPath;
	}

	public ReadWriteCollectionOnDisk<T> getCollection() {
		return collection;
	}

	public void markComplete(Recoverable<?> change) throws BlueDbException {
		String pendingFileName = getPendingFileName(change);
		String completedFileName = getCompletedFileName(change);
		Path pendingPath = Paths.get(historyFolderPath.toString(), pendingFileName);
		Path completedPath = Paths.get(historyFolderPath.toString(), completedFileName);
		FileUtils.moveWithoutLock(pendingPath, completedPath);
	}

	public void markChangePending(Path completedPath) throws BlueDbException {
		String completedPathString = completedPath.toString();
		String pendingPathString = completedPathString.replaceAll(SUFFIX_COMPLETE, SUFFIX_PENDING);
		Path pendingPath = Paths.get(pendingPathString);
		FileUtils.moveWithoutLock(completedPath, pendingPath);
	}

	public List<File> getChangeHistory(long backupStartTime, long backupEndTime) throws BlueDbException {
		List<File> files = FileUtils.getFolderContentsExcludingTempFiles(historyFolderPath, SUFFIX);
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
		List<Recoverable<T>> changes = new ArrayList<>();
		for (File file: getPendingChangeFiles()) {
			Recoverable<T> change = loadPendingChange(file);
			
			if(change != null) {
				changes.add(change);
			} else {
				System.out.println("BlueDB ignoring empty, missing, or corrupt recovery file: " + file.getAbsolutePath());
			}
		}
		Collections.sort(changes);
		return changes;
	}

	public Recoverable<T> loadPendingChange(File file) throws BlueDbException {
		Recoverable<T> change = tryLoadingMassChange(file.toPath());
		if(change == null) {
			change = tryLoadingNormalChange(file.toPath());
		}
		return change;
	}

	private PendingMassChange<T> tryLoadingMassChange(Path path) throws BlueDbException {
		try(OnDiskSortedChangeSupplier<T> onDiskSortedChangeSupplier = new OnDiskSortedChangeSupplier<>(path, fileManager)) {
			TimeStampedFile timeStampedFile = new TimeStampedFile(path.toFile());
			return new PendingMassChange<>(timeStampedFile.getTimestamp(), timeStampedFile.getRecoverableId(), path);
		} catch(Throwable t) {
			return null; //It is expected to fail if this is any other type of change file.
		}
	}

	private Recoverable<T> tryLoadingNormalChange(Path path) {
		try {
			@SuppressWarnings("unchecked")
			Recoverable<T> change = (Recoverable<T>) fileManager.loadObject(path);
			if(change != null) {
				return change;
			}
			return null;
		} catch (Throwable t) {
			return null;
		}
	}

	public DirectoryStream<Path> getCompletedChangeFilesAsStream() throws IOException {
		return FileUtils.getFolderContentsExcludingTempFilesAsStream(historyFolderPath, SUFFIX_COMPLETE);
	}

	public List<File> getPendingChangeFiles() {
		return FileUtils.getFolderContentsExcludingTempFiles(historyFolderPath, SUFFIX_PENDING);
	}

	public void recover() throws BlueDbException {
		for (Recoverable<T> change: getPendingChanges()) {
			change.apply(collection);
			markComplete(change);
		}
	}

	public void placeHoldOnHistoryCleanup() {
		cleaner.placeHoldOnHistoryCleanup();
	}

	public void removeHoldOnHistoryCleanup() {
		cleaner.removeHoldOnHistoryCleanup();
	}

	public long getNewRecoverableId() {
		return lastRecoverableId.getAndIncrement();
	}

	public static String getCompletedFileName(Recoverable<?> change) {
		return  String.valueOf(change.getTimeCreated()) + "." + String.valueOf(change.getRecoverableId()) + SUFFIX_COMPLETE;
	}

	public static String getPendingFileName(Recoverable<?> change) {
		return getPendingFileName(change.getTimeCreated(), change.getRecoverableId());
	}

	public static String getPendingFileName(long creationTime, long recoverableId) {
		return String.valueOf(creationTime) + "." + String.valueOf(recoverableId) + SUFFIX_PENDING;
	}

	public ChangeHistoryCleaner getChangeHistoryCleaner() {
		return cleaner;
	}
}
