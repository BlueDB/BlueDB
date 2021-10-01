package org.bluedb.disk;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * This class allows you to wrap a iterator of one type and specify a mapper function that can
 * flat map the objects to another type while being iterated over.
 * @param <I> The underlying object type being iterated over (Input).
 * @param <O> The visible object type being iterated over (Output).
 */
public class FlatMappingIterator<I, O> implements Iterator<O> {
	private Iterator<I> inputIterator;
	private Function<I, List<O>> mapper;
	
	private LinkedList<O> nextList = new LinkedList<>();

	public FlatMappingIterator(Iterator<I> inputIterator, Function<I, List<O>> mapper) {
		this.inputIterator = inputIterator;
		this.mapper = mapper;
	}

	@Override
	public boolean hasNext() {
		loadNextListIfNecessary();
		return !nextList.isEmpty();
	}
	
	@Override
	public O next() {
		loadNextListIfNecessary();
		return nextList.poll();
	}
	
	private void loadNextListIfNecessary() {
		if(nextList.isEmpty() && inputIterator.hasNext()) {
			nextList.addAll(mapper.apply(inputIterator.next()));
		}
	}
}
