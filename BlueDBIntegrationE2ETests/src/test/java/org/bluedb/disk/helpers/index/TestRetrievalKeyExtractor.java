package org.bluedb.disk.helpers.index;

import java.util.Arrays;
import java.util.List;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.disk.TestValue;

public class TestRetrievalKeyExtractor implements KeyExtractor<IntegerKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<IntegerKey> extractKeys(TestValue object) {
		IntegerKey key = new IntegerKey(object.getCupcakes());
		return Arrays.asList(key);
	}

	@Override
	public Class<IntegerKey> getType() {
		return IntegerKey.class;
	}
}
