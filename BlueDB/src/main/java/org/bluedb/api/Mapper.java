package org.bluedb.api;

import java.io.Serializable;

@FunctionalInterface
public interface Mapper<T extends Serializable> {
	public T update(T object);
}
