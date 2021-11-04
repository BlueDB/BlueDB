package org.bluedb.api.datastructures;

import java.util.Iterator;
import java.util.Set;

public class BlueSimpleInMemorySet<T> implements BlueSimpleSet<T> {
	private final Set<T> set;
	
	public BlueSimpleInMemorySet(Set<T> set) {
		this.set = set;
	}

	@Override
	public Iterator<T> iterator() {
		return set.iterator();
	}

	@Override
	public boolean contains(T object) {
		return set.contains(object);
	}

}
