package org.bluedb.api.datastructures;

import java.util.Set;

public class BlueSimpleInMemorySet<T> implements BlueSimpleSet<T> {
	private final Set<T> set;
	
	public BlueSimpleInMemorySet(Set<T> set) {
		this.set = set;
	}

	@Override
	public BlueSimpleIterator<T> iterator() {
		return new BlueSimpleIteratorWrapper<T>(set.iterator());
	}

	@Override
	public boolean contains(T object) {
		return set.contains(object);
	}

}
