package org.bluedb.disk.helpers.index;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.TestValueWithTimes;

public class TestEndTimeIndexKeyExtractor implements KeyExtractor<LongKey, TestValueWithTimes> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<LongKey> extractKeys(TestValueWithTimes object) {
		LongKey key = new LongKey(object.getEnd());
		return Arrays.asList(key);
	}

	@Override
	public Class<LongKey> getType() {
		return LongKey.class;
	}
}
