package org.bluedb.disk.collection.index.extractors;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.path.SegmentPathManager;

/**
 * Returns a grouping number in all but the first of the segments that the given record key overlaps. Given a timeframe,
 * you should be able to use this index to easily find all of the segments that contain records that started before it 
 * but overlap into it.
 * 
 * Instead of using the segment start grouping numbers we let the grouping numbers be offset in the segments based
 * on the start time of the record. This results in less overlap of keys in the index data which means that the
 * values can be stored in different files while insertion heavy operations are still active and can be rolled up
 * later.
 */
public class OverlappingTimeSegmentsKeyExtractor<V extends Serializable> implements DefaultTimeKeyExtractor<LongTimeKey, V> {

	private static final long serialVersionUID = 1L;

	@Override
	public Class<LongTimeKey> getType() {
		return LongTimeKey.class;
	}
	
	@Override
	public List<LongTimeKey> extractKeys(BlueKey key, ReadableSegmentManager<V> segmentManager) {
		SegmentPathManager pm = segmentManager.getPathManager();
		
		List<LongTimeKey> keys = new LinkedList<>();
		
		long currentGroupingNumber = key.getGroupingNumber() + pm.getSegmentSize(); //Start with a grouping number in the next segment, since we only care about the segments it overlaps into
		long currentSegmentStartGroupingNumber = pm.getSegmentStartGroupingNumber(currentGroupingNumber);
		long currentSegmentEndGroupingNumber = currentSegmentStartGroupingNumber + pm.getSegmentSize() - 1;
		
		while (key.isInRange(currentSegmentStartGroupingNumber, currentSegmentEndGroupingNumber)) {
			keys.add(new LongTimeKey(currentGroupingNumber));
			
			currentGroupingNumber += pm.getSegmentSize();
			currentSegmentStartGroupingNumber += pm.getSegmentSize();
			currentSegmentEndGroupingNumber += pm.getSegmentSize();
		}
		
		return keys;
	}

	@Override
	public List<LongTimeKey> extractKeys(V value) {
		throw new UnsupportedOperationException("DefaultTimeIndexKeyExtractor is a special case that is used for the default time index on time based collections. This method shouldn't be called, it should have special handling.");
	}

}
