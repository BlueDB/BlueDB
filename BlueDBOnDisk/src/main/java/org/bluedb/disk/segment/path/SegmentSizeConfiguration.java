package org.bluedb.disk.segment.path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.bluedb.api.keys.BlueKey;

public class SegmentSizeConfiguration {
	private final Class<? extends BlueKey> keyType;
	private final List<Long> folderSizesTopToBottom;
	private final List<Long> rollupsBottomToTop;
	private final long segmentSize;
	
	public SegmentSizeConfiguration(Class<? extends BlueKey> keyType, List<Long> bottomToTopFolderWidths, List<Long> rollups) {
		this.keyType = keyType;
		this.rollupsBottomToTop = Collections.unmodifiableList(rollups);
		this.folderSizesTopToBottom = Collections.unmodifiableList(bottomUpFolderWidthsToTopDownFolderSizes(bottomToTopFolderWidths));
		this.segmentSize = folderSizesTopToBottom.get(folderSizesTopToBottom.size() - 1);
	}

	public static List<Long> bottomUpFolderWidthsToTopDownFolderSizes(List<Long> bottomToTopWidths) {
		List<Long> result = new ArrayList<>();
		long accumulator = 1;
		for (long iterValue: bottomToTopWidths) {
			accumulator *= iterValue;
			result.add(accumulator);
		}
		Collections.reverse(result);
		return result;
	}
	
	public Class<? extends BlueKey> getKeyType() {
		return keyType;
	}
	
	public List<Long> getFolderSizesTopToBottom() {
		return folderSizesTopToBottom;
	}

	public List<Long> getRollupsBottomToTop() {
		return rollupsBottomToTop;
	}
	
	public long getSegmentSize() {
		return segmentSize;
	}

	@Override
	public String toString() {
		return "SegmentSizeConfiguration [keyType=" + keyType + ", folderSizesTopToBottom=" + folderSizesTopToBottom
				+ ", rollupsBottomToTop=" + rollupsBottomToTop + ", segmentSize=" + segmentSize + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + folderSizesTopToBottom.hashCode();
		result = prime * result + ((keyType == null) ? 0 : keyType.hashCode());
		result = prime * result + rollupsBottomToTop.hashCode();
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
		
		SegmentSizeConfiguration other = (SegmentSizeConfiguration) obj;
		
		if (!Objects.equals(folderSizesTopToBottom, other.folderSizesTopToBottom)) {
			return false;
		}
		
		if (!Objects.equals(keyType, other.keyType)) {
			return false;
		}
		
		if (!Objects.equals(rollupsBottomToTop, other.rollupsBottomToTop)) {
			return false;
		}
		
		return true;
	}
}
