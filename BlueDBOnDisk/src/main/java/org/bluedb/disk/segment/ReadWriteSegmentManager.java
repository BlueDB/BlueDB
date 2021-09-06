package org.bluedb.disk.segment;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.ReadWriteFileManager;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.SortedChangeSupplier;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;
import org.bluedb.disk.segment.rollup.Rollupable;

public class ReadWriteSegmentManager<T extends Serializable> extends ReadableSegmentManager<T> {

	private final ReadWriteFileManager fileManager;

	private final Rollupable rollupable;

	public ReadWriteSegmentManager(Path collectionPath, ReadWriteFileManager fileManager, Rollupable rollupable, SegmentSizeConfiguration sizeConfig) {
		super(collectionPath, sizeConfig);
		this.fileManager = fileManager;
		this.rollupable = rollupable;
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
		return getExistingSegments(allValues);
	}

	@Override
	public List<ReadWriteSegment<T>> getExistingSegments(Range range) {
		return pathManager.getExistingSegmentFiles(range).stream()
				.map((f) -> (toSegment(f.toPath())))
				.sorted()
				.collect(Collectors.toList());
	}

	public void applyChanges(SortedChangeSupplier<T> sortedChangeSupplier) throws BlueDbException {
		sortedChangeSupplier.setCursorToBeginning();
		
		long minStartTime = Long.MIN_VALUE;
		while(sortedChangeSupplier.seekToNextChangeInRange(new Range(minStartTime, Long.MAX_VALUE))) {
			sortedChangeSupplier.setCursorCheckpoint(); //We shouldn't ever have to look at changes before this point again
			
			/*
			 * The first change will help us discover what segment to apply the next changes to. Unless the first change
			 * overlaps into the next segment and we've already handled its first segment. In that case, just move
			 * to the next segment.
			 */
			IndividualChange<T> change = sortedChangeSupplier.getNextChange().get();
			ReadWriteSegment<T> segment = getSegment(Math.max(change.getGroupingNumber(), minStartTime));
			
			segment.applyChanges(sortedChangeSupplier);
			
			minStartTime = segment.getRange().getEnd() + 1; //Don't look at anything before this range
			sortedChangeSupplier.setCursorToLastCheckpoint();
		}
	}
}
