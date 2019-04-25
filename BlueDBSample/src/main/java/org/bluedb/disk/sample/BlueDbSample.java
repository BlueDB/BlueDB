package org.bluedb.disk.sample;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.bluedb.api.BlueCollection;
import org.bluedb.api.BlueDb;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndex;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.BlueDbOnDiskBuilder;
import org.bluedb.disk.sample.model.nontime.NonTimeObject;
import org.bluedb.disk.sample.model.nontime.concrete.NonTimeObjectV1;
import org.bluedb.disk.sample.model.nontime.concrete.NonTimeObjectV2;
import org.bluedb.disk.sample.model.time.TimeObject;
import org.bluedb.disk.sample.model.time.concrete.TimeObjectV1;
import org.bluedb.disk.sample.model.timeframe.TimeFrameObject;
import org.bluedb.disk.sample.model.timeframe.concrete.TimeFrameObjectV1;

public class BlueDbSample {
	private static final String NON_TIME_COLLECTION_NAME = "non-time-objects";
	private static final String TIME_COLLECTION_NAME = "time-objects";
	private static final String TIME_COLLECTION_INDEX_NAME = "data-index";
	private static final String TIMEFRAME_COLLECTION_NAME = "timeframe-objects";
	
	private BlueDb db;
	
	public BlueDbSample() throws BlueDbException {
		db = new BlueDbOnDiskBuilder()
				.setPath(Paths.get("", "sample-db"))
				.build();
		
		db.initializeCollection(NON_TIME_COLLECTION_NAME, UUIDKey.class, NonTimeObject.class, getNonTimeObjectClassesToRegister());
		db.initializeCollection(TIME_COLLECTION_NAME, TimeKey.class, TimeObject.class, getTimeObjectClassesToRegister());
		db.initializeCollection(TIMEFRAME_COLLECTION_NAME, TimeFrameKey.class, TimeFrameObject.class, getTimeframeObjectClassesToRegister());
		
		getTimeCollection().createIndex(TIME_COLLECTION_INDEX_NAME, StringKey.class, new TimeObjectDataIndexExtractor());
	}

	@SuppressWarnings("unchecked")
	private Class<? extends Serializable>[] getNonTimeObjectClassesToRegister() {
		List<Class<? extends Serializable>> classes = Arrays.asList(
				NonTimeObjectV1.class,
				NonTimeObjectV2.class,
				UUID.class
		);
		return (Class<? extends Serializable>[]) classes.toArray();
	}

	@SuppressWarnings("unchecked")
	private Class<? extends Serializable>[] getTimeObjectClassesToRegister() {
		List<Class<? extends Serializable>> classes = Arrays.asList(
				TimeObjectV1.class,
				UUID.class
		);
		return (Class<? extends Serializable>[]) classes.toArray();
	}

	@SuppressWarnings("unchecked")
	private Class<? extends Serializable>[] getTimeframeObjectClassesToRegister() {
		List<Class<? extends Serializable>> classes = Arrays.asList(
				TimeFrameObjectV1.class,
				UUID.class
		);
		return (Class<? extends Serializable>[]) classes.toArray();
	}

	private BlueCollection<NonTimeObject> getNonTimeCollection() throws BlueDbException {
		return db.getCollection(TIME_COLLECTION_NAME, NonTimeObject.class);
	}

	private BlueCollection<TimeObject> getTimeCollection() throws BlueDbException {
		return db.getCollection(TIME_COLLECTION_NAME, TimeObject.class);
	}
	
	private BlueCollection<TimeFrameObject> getTimeframeCollection() throws BlueDbException {
		return db.getCollection(TIME_COLLECTION_NAME, TimeFrameObject.class);
	}

	private BlueIndex<StringKey, TimeObject> getTimeIndex() throws BlueDbException {
		return getTimeCollection().getIndex(TIME_COLLECTION_INDEX_NAME, StringKey.class);
	}
	
	public void insertNonTimeObject(NonTimeObject obj) throws BlueDbException {
		getNonTimeCollection().insert(obj.getKey(), obj);
	}
	
	public void updateDataOnNonTimeObject(NonTimeObject obj, String updatedData) throws BlueDbException {
		getNonTimeCollection().update(obj.getKey(),	itemToUpdate -> itemToUpdate.setData(updatedData));
	}
	
	public boolean containsNotTimeObject(NonTimeObject obj) throws BlueDbException {
		return getNonTimeCollection().contains(obj.getKey());
	}
	
	public void deleteNonTimeObject(NonTimeObject obj) throws BlueDbException {
		getNonTimeCollection().delete(obj.getKey());
	}
	
	public TimeKey getLastTimeObjectKey() throws BlueDbException {
		/*
		 * getLastKey is NOT useful for UUID and String keys since they're not ordered. Long, Int, Time, and TimeFrame keys could give you 
		 * some meaningful data. Long and Int can help you to not create duplicate ids.
		 */
		return (TimeKey) getTimeCollection().getLastKey();
	}
	
	public List<TimeObject> getTimeObjectsByIndex(String data) throws BlueDbException {
		return getTimeIndex().get(new StringKey(data));
	}
	
	public CloseableIterator<TimeFrameObject> queryExample() throws BlueDbException {
		return getTimeframeCollection().query()
				.afterOrAtTime(0)
				.beforeOrAtTime(10)
				.byStartTime() //Only for timeframe objects. If this is in here then it'll only return items that started within the timeframe. Default behavior is to return any item that overlaps the timeframe
				.where(obj -> obj.getData() != null && !obj.getData().isEmpty())
				.getIterator();
	}
	
	public void deleteExample() throws BlueDbException {
		getTimeframeCollection().query()
				.afterOrAtTime(0)
				.beforeOrAtTime(10)
				.where(obj -> obj.getData() != null && !obj.getData().isEmpty())
				.delete();
	}
	
	public void updateExample() throws BlueDbException {
		getTimeframeCollection().query()
				.afterOrAtTime(0)
				.beforeOrAtTime(10)
				.where(obj -> obj.getData() != null && !obj.getData().isEmpty())
				.update(obj -> obj.setData("updated data")); //Will be updating so that whatever you return gets inserted as new object. You don't have to mutate the given object. And you could upgrade it to a newer version.
	}
}
