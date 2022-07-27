package org.bluedb.disk.collection.index.conditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.segment.Range;

public class IncludedSegmentRangeInfo {
	private final Map<Range, Range> rangesByIncludedSegmentRanges = new HashMap<>();
	
	public IncludedSegmentRangeInfo() {
	}
	
	public IncludedSegmentRangeInfo(IncludedSegmentRangeInfo includedSegmentRangeInfoForCondition) {
		StreamUtils.stream(includedSegmentRangeInfoForCondition.getSegmentRangeGroupingNumberRangePairs())
			.forEach(entry -> rangesByIncludedSegmentRanges.put(entry.getKey(), entry.getValue()));
	}

	public boolean isEmpty() {
		return rangesByIncludedSegmentRanges.isEmpty();
	}
	
	public void addIncludedSegmentRangeInfo(Range segmentRange, long groupingNumber) {
		Range currentGroupingNumberRange = rangesByIncludedSegmentRanges.get(segmentRange);
		if(currentGroupingNumberRange == null) {
			rangesByIncludedSegmentRanges.put(segmentRange, new Range(groupingNumber, groupingNumber));
		} else {
			long newGroupingNumberStart = Math.min(currentGroupingNumberRange.getStart(), groupingNumber);
			long newGroupingNumberEnd = Math.max(currentGroupingNumberRange.getEnd(), groupingNumber);
			rangesByIncludedSegmentRanges.put(segmentRange, new Range(newGroupingNumberStart, newGroupingNumberEnd));
		}
	}
	
	public void addIncludedSegmentRangeInfo(Range segmentRange, Range groupingNumberRange) {
		orIncludedSegmentRangeInfo(segmentRange, groupingNumberRange);
	}

	public void combine(IncludedSegmentRangeInfo other, boolean shouldAnd) {
		if(shouldAnd) {
			combineUsingAnd(other);
		} else {
			combineUsingOr(other);
		}
	}

	private void combineUsingAnd(IncludedSegmentRangeInfo other) {
		if(other == null || other.isEmpty()) {
			rangesByIncludedSegmentRanges.clear();
		}
		
		List<SegmentRangeInfo> infoToAdd = new LinkedList<>(); 
		
		Iterator<Entry<Range, Range>> it = rangesByIncludedSegmentRanges.entrySet().iterator();
		while(it.hasNext()) {
			Entry<Range, Range> entry = it.next();
			Range otherGroupingNumberRange = other.getRangeForSegment(entry.getKey());
			if(otherGroupingNumberRange == null || !entry.getValue().overlaps(otherGroupingNumberRange)) {
				/*
				 * We are anding the segments together, so if other doesn't include a segment then remove it.
				 * If other's range doesn't overlap ours then there are no matching values in both.
				 */
				it.remove();
			} else {
				infoToAdd.add(new SegmentRangeInfo(entry.getKey(), otherGroupingNumberRange));
			}
		}
		
		StreamUtils.stream(infoToAdd)
			.forEach(info -> andIncludedSegmentRangeInfo(info.segmentRange, info.groupingNumberRange));
	}

	private void andIncludedSegmentRangeInfo(Range segmentRange, Range groupingNumberRange) {
		Range currentGroupingNumberRange = rangesByIncludedSegmentRanges.get(segmentRange);
		if(currentGroupingNumberRange != null) {
			//Since we're anding, we will only take the overlapping range
			long newGroupingNumberStart = Math.max(currentGroupingNumberRange.getStart(), groupingNumberRange.getStart());
			long newGroupingNumberEnd = Math.max(newGroupingNumberStart, Math.min(currentGroupingNumberRange.getEnd(), groupingNumberRange.getEnd()));
			rangesByIncludedSegmentRanges.put(segmentRange, new Range(newGroupingNumberStart, newGroupingNumberEnd));
		}
	}

	private void combineUsingOr(IncludedSegmentRangeInfo other) {
		if(other == null || other.isEmpty()) {
			return;
		}
		
		for(Entry<Range, Range> otherEntry : other.getSegmentRangeGroupingNumberRangePairs()) {
			orIncludedSegmentRangeInfo(otherEntry.getKey(), otherEntry.getValue());
		}
	}

	private void orIncludedSegmentRangeInfo(Range segmentRange, Range groupingNumberRange) {
		Range currentGroupingNumberRange = rangesByIncludedSegmentRanges.get(segmentRange);
		if(currentGroupingNumberRange == null) {
			rangesByIncludedSegmentRanges.put(segmentRange, new Range(groupingNumberRange.getStart(), groupingNumberRange.getEnd()));
		} else {
			long newGroupingNumberStart = Math.min(currentGroupingNumberRange.getStart(), groupingNumberRange.getStart());
			long newGroupingNumberEnd = Math.max(currentGroupingNumberRange.getEnd(), groupingNumberRange.getEnd());
			rangesByIncludedSegmentRanges.put(segmentRange, new Range(newGroupingNumberStart, newGroupingNumberEnd));
		}
	}
	
	public Set<Entry<Range, Range>> getSegmentRangeGroupingNumberRangePairs() {
		return rangesByIncludedSegmentRanges.entrySet();
	}
	
	public boolean containsSegment(Range segmentRange) {
		return rangesByIncludedSegmentRanges.containsKey(segmentRange);
	}
	
	public Range getRangeForSegment(Range segmentRange) {
		return rangesByIncludedSegmentRanges.get(segmentRange);
	}
	
	public void removeIncludedSegmentRangeInfo(Range segmentRange) {
		rangesByIncludedSegmentRanges.remove(segmentRange);
	}

	@Override
	public String toString() {
		return "IncludedSegmentRangeInfo [rangesByIncludedSegmentRanges=" + rangesByIncludedSegmentRanges + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((rangesByIncludedSegmentRanges == null) ? 0 : rangesByIncludedSegmentRanges.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IncludedSegmentRangeInfo other = (IncludedSegmentRangeInfo) obj;
		if(!Objects.equals(rangesByIncludedSegmentRanges, other.rangesByIncludedSegmentRanges))
			return false;
		return true;
	}
	
	private static class SegmentRangeInfo {
		public Range segmentRange;
		public Range groupingNumberRange;
		
		public SegmentRangeInfo(Range segmentRange, Range groupingNumberRange) {
			this.segmentRange = segmentRange;
			this.groupingNumberRange = groupingNumberRange;
		}
	}
	
}
