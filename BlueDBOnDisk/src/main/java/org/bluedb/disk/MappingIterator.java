package org.bluedb.disk;

import java.util.Iterator;
import java.util.function.Function;

/**
 * This class allows you to wrap a iterator of one type and specify a mapper function that can
 * map the objects to another type while being iterated over.
 * @param <I> The underlying object type being iterated over (Input).
 * @param <O> The visible object type being iterated over (Output).
 */
public class MappingIterator<I, O> implements Iterator<O> {
	private Iterator<I> inputIterator;
	private Function<I, O> mapper;

	public MappingIterator(Iterator<I> inputIterator, Function<I, O> mapper) {
		this.inputIterator = inputIterator;
		this.mapper = mapper;
	}
	
	@Override
	public boolean hasNext() {
		return inputIterator.hasNext();
	}
	
	@Override
	public O next() {
		return mapper.apply(inputIterator.next());
	}
}
