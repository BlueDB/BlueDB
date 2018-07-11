package io.bluedb.disk.collection;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import io.bluedb.disk.segment.ChunkIterator;
import io.bluedb.disk.segment.Segment;
import io.bluedb.disk.serialization.BlueEntity;

public class Bliterator<T extends Serializable> implements Iterator<BlueEntity<T>>, Closeable {

	final private BlueCollectionImpl<T> collection;
	final private List<Segment<T>> segments;
	final private long min;
	final private long max;
	private ChunkIterator<T> chunk;
	private BlueEntity<T> next;

	public Bliterator(final BlueCollectionImpl<T> collection, final long min, final long max) {
		this.collection = collection;
		this.min = min;
		this.max = max;
		segments = collection.getSegmentManager().getExistingSegments(min, max);
	}

	@Override
	public void close() {
		if (chunk != null) {
			chunk.close();
		}
	}

	@Override
	public boolean hasNext() {
		if (next == null) {
			next = nextFromChunk();
		}
		return next != null;
	}

	@Override
	public BlueEntity<T> next() {
		if (next == null) {
			next = nextFromChunk();
		}
		BlueEntity<T> response = next;
		next = null;
		return response;
	}

	protected BlueEntity<T> nextFromChunk() {
		while (!segments.isEmpty() || chunk != null) {
			if (chunk != null && chunk.hasNext()) {
				return chunk.next();
			}
			if (chunk != null) {
				chunk.close();
			}
			chunk = getNextChunk();
		}
		return null;
	}

	protected ChunkIterator<T> getNextChunk() {
		if (segments.isEmpty()) {
			return null;
		}
		Segment<T> segment = segments.remove(0);
		if (segment == null) {
			return null;
		}
		return segment.getIterator(min, max);
	}
}
