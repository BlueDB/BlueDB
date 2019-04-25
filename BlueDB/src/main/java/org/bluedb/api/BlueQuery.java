package org.bluedb.api;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;

public interface BlueQuery<T extends Serializable> {

	BlueQuery<T> where(Condition<T> c);

	BlueQuery<T> byStartTime();

	BlueQuery<T> beforeTime(long time);
	BlueQuery<T> beforeOrAtTime(long time);

	BlueQuery<T> afterTime(long time);
	BlueQuery<T> afterOrAtTime(long time);

	List<T> getList() throws BlueDbException;
	CloseableIterator<T> getIterator() throws BlueDbException;

	void delete() throws BlueDbException;
	void update(Updater<T> updater) throws BlueDbException;
	public int count() throws BlueDbException;
}
