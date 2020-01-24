package org.bluedb.disk.collection.index;

import java.util.List;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.disk.TestValue;

public class NullReturningKeyExtractor implements KeyExtractor<IntegerKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<IntegerKey> extractKeys(TestValue object) {
		return null;
	}

	@Override
	public Class<IntegerKey> getType() {
		return IntegerKey.class;
	}
}
