package org.bluedb.api;

import java.io.Serializable;
import java.util.List;
import org.bluedb.api.exceptions.BlueDbException;

public interface BlueQuery<V extends Serializable> {

	BlueQuery<V> where(Condition<V> c);

	BlueQuery<V> byStartTime();

	BlueQuery<V> beforeTime(long time);
	BlueQuery<V> beforeOrAtTime(long time);

	BlueQuery<V> afterTime(long time);
	BlueQuery<V> afterOrAtTime(long time);

	List<V> getList() throws BlueDbException;
	CloseableIterator<V> getIterator() throws BlueDbException;

	void delete() throws BlueDbException;
	void update(Updater<V> updater) throws BlueDbException;
	void replace(Mapper<V> mapper) throws BlueDbException;
	public int count() throws BlueDbException;
}
