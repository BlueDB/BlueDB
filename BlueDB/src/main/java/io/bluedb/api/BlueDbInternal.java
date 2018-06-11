package io.bluedb.api;

import java.util.List;
import io.bluedb.api.entities.BlueEntity;
import io.bluedb.api.exceptions.BlueDbException;

public interface BlueDbInternal extends BlueDb {
	<T extends BlueEntity> List<T> getAll(Class<T> clazz, List<Condition<T>> conditions) throws BlueDbException;
	<T extends BlueEntity> void deleteAll(Class<T> clazz, List<Condition<T>> conditions) throws BlueDbException;
	<T extends BlueEntity> void updateAll(Class<T> clazz, List<Condition<T>> conditions, Updater<T> updater) throws BlueDbException;
}
