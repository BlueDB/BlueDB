package org.bluedb.disk.segment;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.serialization.BlueEntity;

public class SegmentEntityIterator<T extends Serializable> implements Iterator<BlueEntity<T>>, Closeable {
	
	long highestGroupingNumberCompleted;
	final ReadableSegment<T> segment;
	final long rangeMin;
	final long rangeMax;
	final List<Range> timeRanges;
	BlueObjectInput<BlueEntity<T>> currentInput;
	BlueEntity<T> next = null;
	
	private AtomicBoolean hasClosed = new AtomicBoolean(false);
	
	public SegmentEntityIterator(final ReadableSegment<T> segment, final long highestGroupingNumberCompleted, final long rangeMin, final long rangeMax, boolean enforceRangeStart) {
		this.highestGroupingNumberCompleted = highestGroupingNumberCompleted;
		this.segment = segment;
		this.rangeMin = rangeMin;
		this.rangeMax = rangeMax;
		
		/*
		 * WhereKeyIsIn and Index conditions know exactly what grouping numbers we need to read, so we should
		 * enforce the start range by ignoring files that don't overlap it. Otherwise, we have to make sure to
		 * scan the segment from the beginning so that we don't miss a record that starts before the range
		 * and ends during or after the range.
		 */
		long timeRangeStart = enforceRangeStart ? Math.max(rangeMin, highestGroupingNumberCompleted) : highestGroupingNumberCompleted;
		
		Range timeRange = new Range(timeRangeStart, rangeMax);
		List<File> relevantFiles = segment.getOrderedFilesInRange(timeRange);
		timeRanges = filesToRanges(relevantFiles);
	}

	public SegmentEntityIterator(final ReadableSegment<T> segment, final long rangeMin, final long rangeMax) {
		this(segment, Long.MIN_VALUE, rangeMin, rangeMax, false);
	}

	@Override
	public synchronized void close() {
		if (!hasClosed.getAndSet(true) && currentInput != null) {
			currentInput.close();
		}
	}

	@Override
	public synchronized boolean hasNext() {
		if (hasClosed.get()) {
			throw new RuntimeException("SegmentEntityIterator has already been closed");
		}
		
		if (next == null) {
			next = nextFromFile();
		}
		return next != null;
	}

	@Override
	public synchronized BlueEntity<T> next() {
		if (hasClosed.get()) {
			throw new RuntimeException("SegmentEntityIterator has already been closed");
		}
		
		if (next == null) {
			next = nextFromFile();
		}
		BlueEntity<T> response = next;
		next = null;
		return response;
	}

	public ReadableSegment<T> getSegment() {
		return segment;
	}

	protected BlueEntity<T> nextFromFile() {
		while (true) {
			while (currentInput != null && currentInput.hasNext()) {
				BlueEntity<T> next = currentInput.next();
				BlueKey key = next.getKey();
				if (key.getGroupingNumber() <= highestGroupingNumberCompleted) {
					continue;
				}
				if (key.isInRange(rangeMin, rangeMax)) {
					return next;
				}
				if (key.isAfterRange(rangeMin, rangeMax)) {
					//If we know we're past the max range then there is nothing left to look for in this segment
					return null;
				}
			}
			if (currentInput != null) {
				highestGroupingNumberCompleted = extractMaxGroupingNumber(currentInput);
				currentInput.close();
			}
			currentInput = getNextStream();
			if (currentInput == null) {
				return null;
			}
		}
	}

	protected BlueObjectInput<BlueEntity<T>> getNextStream() {
		Range range;
		while (!timeRanges.isEmpty()) {
			range = timeRanges.remove(0);
			if (highestGroupingNumberCompleted >= range.getEnd()) {
				continue;  // we've already read the rolled up file that includes this range
			}
			try {
				return segment.getObjectInputFor(range.getStart());
			} catch (BlueDbException e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}

	protected Path getCurrentPath() {
		if (currentInput == null) {
			return null;
		} else {
			return currentInput.getPath();
		}
	}

	protected LinkedList<Range> filesToRanges(List<File> files) {
		LinkedList<Range> ranges = new LinkedList<Range>();
		for (File file: files) {
			String fileName = file.getName();
			Range rangeForFile = Range.fromUnderscoreDelmimitedString(fileName);
			ranges.add(rangeForFile);
		}
		return ranges;
	}

	protected static <X extends Serializable> long extractMaxGroupingNumber(BlueObjectInput<BlueEntity<X>> input) {
		Path path = input.getPath();
		String fileName = path.getFileName().toString();
		Range range = Range.fromUnderscoreDelmimitedString(fileName);
		return range.getEnd();
	}
}
