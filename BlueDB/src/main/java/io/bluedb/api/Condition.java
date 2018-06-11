package io.bluedb.api;

import io.bluedb.api.entities.BlueEntity;

@FunctionalInterface
public interface Condition<T extends BlueEntity> {
	public boolean resolve(T entity);
}
