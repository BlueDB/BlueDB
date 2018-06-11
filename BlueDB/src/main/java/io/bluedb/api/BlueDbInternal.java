package io.bluedb.api;

import java.io.Serializable;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;

public interface BlueDbInternal extends BlueDb {
	<T extends Serializable> List<T> getAll(Class<T> clazz, List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException;

	<T extends Serializable> void deleteAll(Class<T> clazz, List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions) throws BlueDbException;

	<T extends Serializable> void updateAll(Class<T> clazz, List<Condition<BlueKey>> keyConditions, List<Condition<T>> objectConditions, Updater<T> updater) throws BlueDbException;
}
