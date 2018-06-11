package io.bluedb.api;

import io.bluedb.api.entities.BlueEntity;

@FunctionalInterface
public interface Updater<T extends BlueEntity> {
	public void update(T entity);
}
