package org.bluedb.disk.recovery;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectInput.BlueObjectInputState;
import org.bluedb.disk.file.BlueSeekableInputStream;
import org.bluedb.disk.file.ReadFileManager;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.segment.Range;

public class OnDiskSortedChangeSupplier<T extends Serializable> implements SortedChangeSupplier<T> {
	private ReadFileManager fileManager;
	private BlueReadLock<Path> changeFileReadLock;
	
	private BlueSeekableInputStream changesSeekableInputStream;
	private BlueObjectInput<IndividualChange<T>> changesObjectInputStream;
	private boolean hasCalledFirstSeek = false;
	private boolean shouldSkipNextInSeek = false;
	
	private CursorInfo<T> firstChangeCursorInfo;
	private CursorInfo<T> lastCheckpointCursorInfo;
	
	public OnDiskSortedChangeSupplier(Path changesFilePath, ReadFileManager fileManager) throws BlueDbException {
		try {
			this.fileManager = fileManager;
			
			changeFileReadLock = fileManager.getReadLockIfFileExists(changesFilePath);
			changesSeekableInputStream = new BlueSeekableInputStream(changesFilePath);
			BlueFileMetadata metadata = fileManager.readMetadata(changesSeekableInputStream);
			if(isLegacyFileManagerFile(metadata)) {
				throw new BlueDbException("Failed to open change file as OnDiskSortedChangeSupplier. It is a legacy file manager file.");
			}
			loadDataForUpToDataChangeFile(metadata);
		} catch(Throwable t) {
			close();
			throw t;
		}
	}

	private boolean isLegacyFileManagerFile(BlueFileMetadata metadata) {
		return metadata == null || !metadata.isTrue(BlueFileMetadataKey.SORTED_MASS_CHANGE_FILE);
	}

	private void loadDataForUpToDataChangeFile(BlueFileMetadata metadata) throws BlueDbException {
		this.changesObjectInputStream = fileManager.getBlueInputStream(changeFileReadLock, changesSeekableInputStream);
		this.firstChangeCursorInfo = getCurrentCursorInfo();
		this.lastCheckpointCursorInfo = firstChangeCursorInfo;
	}
	
	@Override
	public boolean seekToNextChangeInRange(Range range) throws BlueDbException {
		if(shouldSkipNextInSeek()) {
			changesObjectInputStream.next(); //Skip the item we're pointing at unless we haven't done our first seek yet
		}
		
		while(changesObjectInputStream.hasNext()) {
			IndividualChange<T> change = changesObjectInputStream.peek();
			if(change.getKey().isInRange(range.getStart(), range.getEnd())) {
				return true;
			} else if(change.getKey().isAfterRange(range.getStart(), range.getEnd())) {
				return false;
			} else {
				changesObjectInputStream.next();
			}
		}
		
		return false;
	}

	private boolean shouldSkipNextInSeek() {
		boolean result = changesObjectInputStream.hasNext() && (hasCalledFirstSeek && shouldSkipNextInSeek);
		hasCalledFirstSeek = true;
		shouldSkipNextInSeek = true;
		return result;
	}

	@Override
	public Optional<IndividualChange<T>> getNextChange() throws BlueDbException {
		if(!hasCalledFirstSeek) {
			return Optional.empty();
		}
		
		return Optional.ofNullable(changesObjectInputStream.peek());
	}

	@Override
	public boolean hasMoreThanOneChangeLeftInRange(Range range) throws BlueDbException {
		CursorInfo<T> originalCursorInfo = getCurrentCursorInfo();
		
		try {
			int inRangeCount = 0;
			
			while(changesObjectInputStream.hasNext()) {
				IndividualChange<T> change = changesObjectInputStream.next();
				if(change.getKey().isInRange(range.getStart(), range.getEnd())) {
					inRangeCount++;
				}
				
				if(inRangeCount > 1) {
					return true;
				}
			}
		} finally {
			setCursorInfo(originalCursorInfo, true);
		}
		
		return false;
	}

	@Override
	public Set<Long> findGroupingNumbersForNextChangesBeforeOrAtGroupingNumber(long maxGroupingNumber) throws BlueDbException {
		Set<Long> groupingNumbers = new HashSet<>();
		
		CursorInfo<T> originalCursorInfo = getCurrentCursorInfo();
		try {
			while(changesObjectInputStream.hasNext()) {
				IndividualChange<T> change = changesObjectInputStream.next();
				if(change.getKey().getGroupingNumber() <= maxGroupingNumber) {
					groupingNumbers.add(change.getKey().getGroupingNumber());
				} else {
					break; //The changes are sorted so once we are after the maxGroupingNumber we can stop looking.
				}
			}
		} finally {
			setCursorInfo(originalCursorInfo, true);
		}
		
		return groupingNumbers;
	}

	@Override
	public void setCursorCheckpoint() throws BlueDbException {
		this.lastCheckpointCursorInfo = getCurrentCursorInfo();
	}

	@Override
	public void setCursorToLastCheckpoint() throws BlueDbException {
		setCursorInfo(lastCheckpointCursorInfo, false);
	}

	@Override
	public void setCursorToBeginning() throws BlueDbException {
		setCursorInfo(firstChangeCursorInfo, false);
	}
	
	@Override
	public void close() {
		if(changeFileReadLock != null) {
			changeFileReadLock.release();
		}
		if(changesObjectInputStream != null) {
			changesObjectInputStream.close();
		}
		if(changesSeekableInputStream != null) {
			changesSeekableInputStream.close();
		}
	}
	
	private static class CursorInfo<T extends Serializable> {
		public boolean hasCalledFirstSeek;
		public long cursorPosition;
		public BlueObjectInputState<IndividualChange<T>> objectInputState;
		
		public CursorInfo(boolean hasCalledFirstSeek, long cursorPosition, BlueObjectInputState<IndividualChange<T>> objectInputState) {
			this.hasCalledFirstSeek = hasCalledFirstSeek;
			this.cursorPosition = cursorPosition;
			this.objectInputState = objectInputState;
		}
	}

	private CursorInfo<T> getCurrentCursorInfo() throws BlueDbException {
		return new CursorInfo<T>(hasCalledFirstSeek, changesSeekableInputStream.getCursorPosition(), changesObjectInputStream.getState());
	}
	
	private void setCursorInfo(CursorInfo<T> cursorInfo, boolean shouldSkipNextInSeek) throws BlueDbException {
		hasCalledFirstSeek = cursorInfo.hasCalledFirstSeek;
		this.shouldSkipNextInSeek = shouldSkipNextInSeek;
		changesSeekableInputStream.setCursorPosition(cursorInfo.cursorPosition);
		changesObjectInputStream.setState(cursorInfo.objectInputState);
	}

}
