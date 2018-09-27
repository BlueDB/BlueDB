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
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.serialization.BlueEntity;

public class SegmentEntityIterator<T extends Serializable> implements Iterator<BlueEntity<T>>, Closeable {

	long highestGroupingNumberCompleted;
	final Segment<T> segment;
	final long rangeMin;
	final long rangeMax;
	final List<Range> timeRanges;
	BlueObjectInput<BlueEntity<T>> currentInput;
	BlueEntity<T> next = null;
	
	public SegmentEntityIterator(final Segment<T> segment, final long highestGroupingNumberCompleted, final long rangeMin, final long rangeMax) {
		this.highestGroupingNumberCompleted =highestGroupingNumberCompleted;
		this.segment = segment;
		this.rangeMin = rangeMin;
		this.rangeMax = rangeMax;
		Range timeRange = new Range(highestGroupingNumberCompleted, rangeMax);
		List<File> relevantFiles = segment.getOrderedFilesInRange(timeRange);
		timeRanges = filesToRanges(relevantFiles);
	}

	public SegmentEntityIterator(final Segment<T> segment, final long rangeMin, final long rangeMax) {
		this(segment, Long.MIN_VALUE, rangeMin, rangeMax);
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

	public Segment<T> getSegment() {
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
