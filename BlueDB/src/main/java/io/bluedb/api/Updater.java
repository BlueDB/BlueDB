package io.bluedb.api;

import java.io.Serializable;

@FunctionalInterface
public interface Updater<T extends Serializable> {
	public void update(T entity);
}
