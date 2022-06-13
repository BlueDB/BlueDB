package org.bluedb.disk;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UncheckedBlueDbException;

/**
 * This class allows you to wrap an iterator of one type to provide mappings,
 * flat mappings, filters, validations, etc. that can be applied while iterating
 * over the contents.
 * @param <I> The data type being iterated over by the wrapped iterator (Input).
 * @param <O> The data type being iterated over (Output).
 */
public class IteratorWrapper<I, O> implements Iterator<O> {
	private final Iterator<I> inputIterator;
	
	private final List<IteratorWrapperFilter<I>> filters = new LinkedList<>();
	private final List<IteratorWrapperValidator<I>> validators = new LinkedList<>();
	
	private final IteratorWrapperMapper<I, O> mapper;
	private final IteratorWrapperFlatMapper<I, O> flatMapper;
	private final Class<O> outputClassType;
	
	private final LinkedList<O> nextList = new LinkedList<>();

	public IteratorWrapper(Iterator<I> inputIterator, Class<O> outputClassType) {
		this.inputIterator = inputIterator;
		this.mapper = null;
		this.flatMapper = null;
		this.outputClassType = outputClassType;
	}

	public IteratorWrapper(Iterator<I> inputIterator, IteratorWrapperMapper<I, O> mapper) {
		this.inputIterator = inputIterator;
		this.mapper = mapper;
		this.flatMapper = null;
		this.outputClassType = null;
	}
	
	public IteratorWrapper(Iterator<I> inputIterator, IteratorWrapperFlatMapper<I, O> flatMapper) {
		this.inputIterator = inputIterator;
		this.mapper = null;
		this.flatMapper = flatMapper;
		this.outputClassType = null;
	}
	
	public IteratorWrapper<I, O> addFilter(IteratorWrapperFilter<I> filter) {
		this.filters.add(filter);
		return this;
	}
	
	public IteratorWrapper<I, O> addValidator(IteratorWrapperValidator<I> validator) {
		this.validators.add(validator);
		return this;
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
	
	@SuppressWarnings("unchecked")
	private void loadNextListIfNecessary() throws UncheckedBlueDbException {
		try {
			while(nextList.isEmpty() && inputIterator.hasNext()) {
				I nextInput = inputIterator.next();
				if(shouldAccept(nextInput)) {
					validate(nextInput);
					if(mapper != null) {
						nextList.add(mapper.map(nextInput));
					} else if(flatMapper != null) {
						nextList.addAll(flatMapper.flatMap(nextInput));
					} else if(outputClassType != null) {
						if(outputClassType.isAssignableFrom(nextInput.getClass())) {
							nextList.add((O) nextInput);
						} else {
							BlueDbException e = new BlueDbException("A mapper must be supplied in order to wrap an iterator whose type can't be assigned to the outer iterator type.");
							throw new UncheckedBlueDbException(e);
						}
					}
				} else {
					continue;
				}
			}
		} catch (BlueDbException e) {
			throw new UncheckedBlueDbException(e);
		}
	}

	private boolean shouldAccept(I inputObject) throws BlueDbException {
		for(IteratorWrapperFilter<I> filter : filters) {
			if(!filter.accepts(inputObject)) {
				return false;
			}
		}
		return true;
	}
	
	private void validate(I inputObject) throws BlueDbException {
		for(IteratorWrapperValidator<I> validator : validators) {
			validator.validate(inputObject);
		}
	}

	@FunctionalInterface
	public interface IteratorWrapperMapper<I, O> {
		public O map(I object) throws BlueDbException;
	}
	
	@FunctionalInterface
	public interface IteratorWrapperFlatMapper<I, O> {
		public List<O> flatMap(I object) throws BlueDbException;
	}
	
	@FunctionalInterface
	public interface IteratorWrapperFilter<I> {
		public boolean accepts(I object) throws BlueDbException;
	}
	
	@FunctionalInterface
	public interface IteratorWrapperValidator<I> {
		public void validate(I object) throws BlueDbException;
	}
}
