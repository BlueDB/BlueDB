package org.bluedb.disk.segment;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;

public enum SegmentSizeSettings {
	TIME_1_HOUR(asList(1L, 6000L, 3_600_000L), asList(3_600_000L, 24L, 30L, 12L)),
	HASH_DEFAULT(asList(1L, 524288L), asList(524288L, 128L, 64L)),
	INT_DEFAULT(asList(1L, 256L), asList(256L, 64L, 64L, 64L)),
	LONG_DEFAULT(asList(1L, 64L), asList(64L, 256L, 512L, 512L, 512L, 256L, 128L)),
	;

	private final List<Long> folderSizesTopToBottom;
	private final List<Long> rollupsBottomToTop;
	
	SegmentSizeSettings(List<Long> rollups, List<Long> bottomToTopFolderWidths) {
		this.rollupsBottomToTop = Collections.unmodifiableList(rollups);
		this.folderSizesTopToBottom = Collections.unmodifiableList(bottomUpFolderWidthsToTopDownFolderSizes(bottomToTopFolderWidths));
	}

	public List<Long> getFolderSizes() {
		return folderSizesTopToBottom;
	}

	public List<Long> getRollupSizes() {
		return rollupsBottomToTop;
	}

	public long getSegmentSize() {
		return folderSizesTopToBottom.get(folderSizesTopToBottom.size() - 1);
	}

	public static SegmentSizeSettings chooseSegmentSize(Class<? extends BlueKey> keyType) {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return TIME_1_HOUR;
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return LONG_DEFAULT;
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			return INT_DEFAULT;
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return HASH_DEFAULT;
		} else {
			throw new UnsupportedOperationException("Cannot create a SegmentPathManager for type " + keyType);
		}
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
}
