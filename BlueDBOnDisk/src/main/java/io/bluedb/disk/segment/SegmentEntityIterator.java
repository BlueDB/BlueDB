package io.bluedb.disk.segment;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.serialization.BlueEntity;

public class SegmentEntityIterator<T extends Serializable> implements Iterator<BlueEntity<T>>, Closeable {

	long highestGroupingNumberCompleted;
	final Segment<T> segment;
	final long min;
	final long max;
	final List<Range> timeRanges;
	BlueObjectInput<BlueEntity<T>> currentInput;
	BlueEntity<T> next = null;
	
	public SegmentEntityIterator(final Segment<T> segment, final long min, final long max) {
		highestGroupingNumberCompleted = Long.MIN_VALUE;
		this.segment = segment;
		this.min = min;
		this.max = max;
		// We can't bound from below.  A query for [2,4] should return a TimeRangeKey [1,3] which would be stored at 1.
		Range timeRange = new Range(Long.MIN_VALUE, max);
		List<File> relevantFiles = segment.getOrderedFilesInRange(timeRange);
		timeRanges = filesToRanges(relevantFiles);
	}

	@Override
	public void close() {
		if (currentInput != null) {
			currentInput.close();
		}
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			next = nextFromFile();
		}
		return next != null;
	}

	@Override
	public BlueEntity<T> next() {
		if (next == null) {
			next = nextFromFile();
		}
		BlueEntity<T> response = next;
		next = null;
		return response;
	}

	protected BlueEntity<T> nextFromFile() {
		while (true) {
			while (currentInput != null && currentInput.hasNext()) {
				BlueEntity<T> next = currentInput.next();
				BlueKey key = next.getKey();
				if (key.getGroupingNumber() <= highestGroupingNumberCompleted) {
					continue;
				}
				if (Blutils.isInRange(key, min, max)) {
					return next;
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
