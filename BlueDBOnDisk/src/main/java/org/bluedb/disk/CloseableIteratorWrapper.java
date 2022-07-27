package org.bluedb.disk;

import org.bluedb.api.CloseableIterator;

/**
 * This class allows you to wrap a closeable iterator of one type to provide mappings,
 * flat mappings, filters, validations, etc. that can be applied while iterating
 * over the contents.
 * @param <I> The data type being iterated over by the wrapped iterator (Input).
 * @param <O> The data type being iterated over (Output).
 */
public class CloseableIteratorWrapper<I, O> extends IteratorWrapper<I, O> implements CloseableIterator<O> {
	private final CloseableIterator<I> inputIterator;
	
	public CloseableIteratorWrapper(CloseableIterator<I> inputIterator, Class<O> outputClassType) {
		super(inputIterator, outputClassType);
		this.inputIterator = inputIterator;
	}

	public CloseableIteratorWrapper(CloseableIterator<I> inputIterator, IteratorWrapperMapper<I, O> mapper) {
		super(inputIterator, mapper);
		this.inputIterator = inputIterator;
	}
	
	public CloseableIteratorWrapper(CloseableIterator<I> inputIterator, IteratorWrapperFlatMapper<I, O> flatMapper) {
		super(inputIterator, flatMapper);
		this.inputIterator = inputIterator;
	}

	@Override
	public void keepAlive() {
		inputIterator.keepAlive();
	}

	@Override
	public void close() {
		inputIterator.close();
	}
	
	@Override
	public CloseableIteratorWrapper<I, O> addFilter(IteratorWrapperFilter<I> filter) {
		super.addFilter(filter);
		return this;
	}
	
	@Override
	public CloseableIteratorWrapper<I, O> addValidator(IteratorWrapperValidator<I> validator) {
		super.addValidator(validator);
		return this;
	}
}
