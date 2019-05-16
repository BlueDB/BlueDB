package org.bluedb.disk.segment;

import static java.util.Arrays.asList;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;

public enum SegmentSizeSettings {
	TIME_1_HOUR(TimeKey.class, asList(1L, 6000L, 3_600_000L), asList(3_600_000L, 24L, 30L, 12L)),
	LONG_DEFAULT(LongKey.class, asList(1L, 64L), asList(64L, 256L, 512L, 512L, 512L, 256L, 128L)),
	INT_DEFAULT(IntegerKey.class, asList(1L, 256L), asList(256L, 64L, 64L, 64L)),
	HASH_DEFAULT(HashGroupedKey.class, asList(1L, 524288L), asList(524288L, 128L, 64L)),
	;

	Class<? extends BlueKey> keyType;
	private final List<Long> folderSizesTopToBottom;
	private final List<Long> rollupsBottomToTop;
	private final long segmentSize;
	
	SegmentSizeSettings(Class<? extends BlueKey> keyType, List<Long> rollups, List<Long> bottomToTopFolderWidths) {
		this.keyType = keyType;
		this.rollupsBottomToTop = Collections.unmodifiableList(rollups);
		this.folderSizesTopToBottom = Collections.unmodifiableList(bottomUpFolderWidthsToTopDownFolderSizes(bottomToTopFolderWidths));
		segmentSize = folderSizesTopToBottom.get(folderSizesTopToBottom.size() - 1);
	}

	public List<Long> getFolderSizes() {
		return folderSizesTopToBottom;
	}

	public List<Long> getRollupSizes() {
		return rollupsBottomToTop;
	}

	public long getSegmentSize() {
		return segmentSize;
	}

	public static SegmentSizeSettings getDefaultSettingsFor(Class<? extends BlueKey> keyType) {
		if (TimeKey.class.isAssignableFrom(keyType)) {
			return TIME_1_HOUR;
		} else if (LongKey.class.isAssignableFrom(keyType)) {
			return LONG_DEFAULT;
		} else if (IntegerKey.class.isAssignableFrom(keyType)) {
			return INT_DEFAULT;
		} else if (HashGroupedKey.class.isAssignableFrom(keyType)) {
			return HASH_DEFAULT;
		} else {
			throw new UnsupportedOperationException("No " + SegmentSizeSettings.class.getSimpleName() + " for " + keyType);
		}
	}

	public static long getDefaultSegmentSizeFor(Class<? extends BlueKey> keyType) {
		return getDefaultSettingsFor(keyType).getSegmentSize();
	}

	public static SegmentSizeSettings getSettings(Class<? extends BlueKey> keyType, long segmentSize) {
		for (SegmentSizeSettings option: values()) {
			if (option.keyType.isAssignableFrom(keyType) && option.getSegmentSize() == segmentSize) {
				return option;
			}
		}
		throw new InvalidParameterException("No " + SegmentSizeSettings.class.getSimpleName() + " for " + keyType + " and size " + segmentSize + ".");
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
