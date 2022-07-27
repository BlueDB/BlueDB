package org.bluedb.disk.collection.task;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.bluedb.api.Condition;
import org.bluedb.api.datastructures.BlueKeyValuePair;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.IteratorWrapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperMapper;
import org.bluedb.disk.collection.CollectionEntityIterator;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.collection.index.conditions.IncludedSegmentRangeInfo;
import org.bluedb.disk.file.FileUtils;
import org.bluedb.disk.query.QueryIndexConditionGroup;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.recovery.OnDiskSortedChangeSupplier;
import org.bluedb.disk.recovery.PendingMassChange;
import org.bluedb.disk.recovery.RecoveryManager;
import org.bluedb.disk.recovery.SortedChangeIterator;
import org.bluedb.disk.segment.Range;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.bluedb.disk.serialization.BlueEntity;

public abstract class BatchUpsertChangeTask<T extends Serializable> extends QueryTask {
	
	private final ReadWriteCollectionOnDisk<T> collection;
	private final ReadableSegmentManager<T> segmentManager;
	private final SegmentPathManager pathManager;
	private final RecoveryManager<T> recoveryManager;
	
	private final Iterator<BlueKeyValuePair<T>> keyValuePairIterator;
	
	private final IncludedSegmentRangeInfo includedSegmentRangeInfo = new IncludedSegmentRangeInfo();
	private long minIncludedRangeTime = Long.MAX_VALUE;
	private long maxIncludedRangeTime = Long.MIN_VALUE;
	
	public BatchUpsertChangeTask(String description, ReadWriteCollectionOnDisk<T> collection, Iterator<BlueKeyValuePair<T>> keyValuePairIterator) {
		super(description);
		this.collection = collection;
		this.segmentManager = collection.getSegmentManager();
		this.pathManager = segmentManager.getPathManager();
		this.recoveryManager = collection.getRecoveryManager();
		this.keyValuePairIterator = keyValuePairIterator;
	}

	@Override
	public void execute() throws BlueDbException {
		if(!keyValuePairIterator.hasNext()) {
			return;
		}
		
		PendingMassChange<T> insertChanges = createTempMassInsertChangeFileAndUpdateIncludedSegmentInfo();
		try {
			PendingMassChange<T> finalMassChange;
			try(CollectionEntityIterator<T> entitiesInRangeIterator = createIteratorForAllEntitiesInIncludedRanges();
					OnDiskSortedChangeSupplier<T> insertChangeSupplier = new OnDiskSortedChangeSupplier<>(insertChanges.getChangesFilePath(), collection.getFileManager())) {
				SortedChangeIterator<T> insertChangeIterator = new SortedChangeIterator<>(insertChangeSupplier);
				
				IteratorWrapperMapper<IndividualChange<T>, IndividualChange<T>> finalChangeMapper = insertChange -> findMatchingEntityAndMapInsertToUpdateIfSuccessful(entitiesInRangeIterator, insertChange);
				Iterator<IndividualChange<T>> finalChangeIterator = new IteratorWrapper<>(insertChangeIterator, finalChangeMapper);
				
				finalMassChange = recoveryManager.saveMassChangeForBatchUpsert(finalChangeIterator);
			}
			
			finalMassChange.apply(collection);
			recoveryManager.markComplete(finalMassChange);
		} finally {
			FileUtils.deleteIfExistsWithoutLock(insertChanges.getChangesFilePath());
		}
	}

	private PendingMassChange<T> createTempMassInsertChangeFileAndUpdateIncludedSegmentInfo() throws BlueDbException {
		IteratorWrapperMapper<BlueKeyValuePair<T>, IndividualChange<T>> mapper = this::createInsertChangeAndUpdateIncludedSegmentInfo;
		Iterator<IndividualChange<T>> insertChangeIterator = new IteratorWrapper<>(keyValuePairIterator, mapper)
				.addValidator(keyValuePair -> ensureCorrectKeyType(keyValuePair.getKey()));
		PendingMassChange<T> changeBatch = recoveryManager.saveTempMassChangeForUnorderedChanges(insertChangeIterator);
		return changeBatch;
	}
	
	private IndividualChange<T> createInsertChangeAndUpdateIncludedSegmentInfo(BlueKeyValuePair<T> keyValuePair) {
		Range segmentRangeForKey = segmentManager.toRange(pathManager.getSegmentPath(keyValuePair.getKey()));
		includedSegmentRangeInfo.addIncludedSegmentRangeInfo(segmentRangeForKey, keyValuePair.getKey().getGroupingNumber());
		if(segmentRangeForKey.getStart() < minIncludedRangeTime) {
			minIncludedRangeTime = segmentRangeForKey.getStart();
		}
		if(segmentRangeForKey.getEnd() > maxIncludedRangeTime) {
			maxIncludedRangeTime = segmentRangeForKey.getEnd();
		}
		
		return IndividualChange.createInsertChange(keyValuePair);
	}
	
	private void ensureCorrectKeyType(BlueKey key) throws BlueDbException {
		if (!collection.getKeyType().isAssignableFrom(key.getClass())) {
			throw new BlueDbException("wrong key type (" + key.getClass() + ") for Collection with key type " + collection.getKeyType());
		}
	}

	private CollectionEntityIterator<T> createIteratorForAllEntitiesInIncludedRanges() {
		Range range = new Range(minIncludedRangeTime, maxIncludedRangeTime);
		List<QueryIndexConditionGroup<T>> indexConditionGroups = new LinkedList<>();
		List<Condition<T>> objectConditions = new LinkedList<>();
		List<Condition<BlueKey>> keyConditions = new LinkedList<>();
		
		return new CollectionEntityIterator<T>(segmentManager, range, true, indexConditionGroups, objectConditions, keyConditions, Optional.of(includedSegmentRangeInfo));
	}
	
	private IndividualChange<T> findMatchingEntityAndMapInsertToUpdateIfSuccessful(CollectionEntityIterator<T> entitiesInRangeIterator, IndividualChange<T> insertChange) throws BlueDbException {
		while(entitiesInRangeIterator.hasNext()) {
			BlueEntity<T> nextEntityInRange = entitiesInRangeIterator.peek();
			
			int compareTo = nextEntityInRange.getKey().compareTo(insertChange.getKey());
			
			if(compareTo > 0) {
				break; //We're passed the insert change so we won't find an existing record for it
			}
			
			//We can go ahead and advance the iterator now since we know that we haven't passed what we're looking for 
			entitiesInRangeIterator.next();
			
			if(compareTo == 0) {
				//This should be an update change for this entity instead of an insert
				BlueEntity<T> newEntity = new BlueEntity<T>(insertChange.getKey(), insertChange.getNewValue());
				return createUpdateChange(nextEntityInRange, newEntity);
			}
		}
		
		//There was no existing record for this key so this is truly an insert change
		return insertChange;
	}
	
	protected abstract IndividualChange<T> createUpdateChange(BlueEntity<T> oldEntity, BlueEntity<T> newEntity) throws BlueDbException;
}
