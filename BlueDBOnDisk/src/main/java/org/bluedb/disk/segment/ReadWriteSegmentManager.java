package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;
import org.bluedb.disk.segment.rollup.Rollupable;

public class ReadWriteSegmentManager<T extends Serializable> extends ReadableSegmentManager<T> {

	private final ReadWriteFileManager fileManager;

	private final Rollupable rollupable;
	
	private final boolean saveDuplicateRecordsInEachSegment;

	public ReadWriteSegmentManager(Path collectionPath, ReadWriteFileManager fileManager, Rollupable rollupable, SegmentSizeConfiguration sizeConfig, boolean saveDuplicateRecordsInEachSegment) {
		super(collectionPath, sizeConfig);
		this.fileManager = fileManager;
		this.rollupable = rollupable;
		this.saveDuplicateRecordsInEachSegment = saveDuplicateRecordsInEachSegment;
	}

	protected ReadWriteSegment<T> toSegment(Path path) {
		Range range = toRange(path);
		return new ReadWriteSegment<T>(path, range, rollupable, getFileManager(), pathManager.getRollupLevels());
	}

	public ReadWriteFileManager getFileManager() {
		return fileManager;
	}

	@Override
	public ReadWriteSegment<T> getFirstSegment(BlueKey key) {
		long groupingNumber = key.getGroupingNumber();
		return getSegment(groupingNumber);
	}

	@Override
	public ReadWriteSegment<T> getSegment(long groupingNumber) {
		Path segmentPath = pathManager.getSegmentPath(groupingNumber);
		return toSegment(segmentPath);
	}

	public List<ReadWriteSegment<T>> getAllSegments(BlueKey key) {
		return pathManager.getAllPossibleSegmentPaths(key).stream()
				.map((p) -> (toSegment(p)))
				.collect(Collectors.toList());
	}

	@Override
	public List<ReadWriteSegment<T>> getAllExistingSegments() {
		Range allValues = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		return getExistingSegments(allValues, Optional.empty());
	}

	@Override
	public List<ReadWriteSegment<T>> getExistingSegments(Range range, Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toSegment(f.toPath())))
				.filter(s -> !includedSegmentRangeInfo.isPresent() || includedSegmentRangeInfo.get().containsSegment(s.getRange()))
				.sorted()
				.collect(Collectors.toList());
	}

	@Override
	public List<Range> getExistingSegmentRanges(Range range, Optional<IncludedSegmentRangeInfo> includedSegmentRangeInfo) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toRange(f.toPath())))
				.filter(r -> !includedSegmentRangeInfo.isPresent() || includedSegmentRangeInfo.get().containsSegment(r))
				.sorted()
				.collect(Collectors.toList());
	}

	public void applyChanges(SortedChangeSupplier<T> sortedChangeSupplier) throws BlueDbException {
		sortedChangeSupplier.setCursorToBeginning();
		
		Range range = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
		while(sortedChangeSupplier.nextChangeOverlapsRange(range) || sortedChangeSupplier.seekToNextChangeInRange(range)) {
			if(saveDuplicateRecordsInEachSegment) {
				sortedChangeSupplier.setCursorCheckpoint(); //We shouldn't ever have to look at changes before this point again
			}
			
			/*
			 * The first change will help us discover what segment to apply the next changes to. Unless the first change
			 * overlaps into the next segment and we've already handled its first segment. In that case, just move
			 * to the next segment.
			 */
			IndividualChange<T> change = sortedChangeSupplier.getNextChange().get();
			ReadWriteSegment<T> segment = getSegment(Math.max(change.getGroupingNumber(), range.getStart()));
			
			segment.applyChanges(sortedChangeSupplier);
			range = new Range(segment.getRange().getEnd() + 1, Long.MAX_VALUE); //Don't look at anything before this range
			
			if(saveDuplicateRecordsInEachSegment) {
				sortedChangeSupplier.setCursorToLastCheckpoint();
			}
		}
	}
}
