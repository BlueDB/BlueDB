package io.bluedb.disk.segment;

import java.io.Closeable;
import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.file.BlueObjectInput;
import io.bluedb.disk.serialization.BlueEntity;

public class ChunkIterator<T extends Serializable> implements Iterator<BlueEntity<T>>, Closeable {

	long highestGroupingNumberPutIntoStream;
	final Segment<T> segment;
	final long min;
	final long max;
	final List<TimeRange> timeRanges;
	TimeRange currentRange;
	BlueObjectInput<BlueEntity<T>> currentInput;
	BlueEntity<T> next = null;
	
	public ChunkIterator(final Segment<T> segment, final long min, final long max) {
		highestGroupingNumberPutIntoStream = Long.MIN_VALUE;
		this.segment = segment;
		this.min = min;
		this.max = max;
		// We can't bound from below.  A query for [2,4] should return a TimeRangeKey [1,3] which would be stored at 1.
		TimeRange timeRange = new TimeRange(Long.MIN_VALUE, max);
		List<File> relevantFiles = segment.getOrderedFilesInRange(timeRange);
		timeRanges = relevantFiles.stream().map((f) -> TimeRange.fromUnderscoreDelmimitedString(f.getName())).collect(Collectors.toList());
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
		BlueEntity<T> nextFromFile = null;
		while (nextFromFile == null) {
			while (currentInput != null && currentInput.hasNext()) {
				BlueEntity<T> next = currentInput.next();
				BlueKey key = next.getKey();
				if (key.getGroupingNumber() < highestGroupingNumberPutIntoStream) {
					continue;
				}
				if (Blutils.isInRange(key, min, max)) {
					return next;
				}
				highestGroupingNumberPutIntoStream = currentRange.getEnd() + 1;
			}
			if (currentInput != null) {
				currentInput.close();
			}
			currentInput = getNextStream();
			if (currentInput == null) {
				return null;
			}
		}
		return nextFromFile;
	}

	protected BlueObjectInput<BlueEntity<T>> getNextStream() {
		TimeRange range;
		while (!timeRanges.isEmpty()) {
			range = timeRanges.remove(0);
			if (highestGroupingNumberPutIntoStream > range.getEnd()) {
				continue;  // we've already read the rolled up file that includes this range
			}
			currentRange = range;
			try {
				return segment.getObjectInputFor(range.getStart());
			} catch (BlueDbException e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}
}
