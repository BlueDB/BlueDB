package org.bluedb.api.datastructures;

import static org.junit.Assert.assertEquals;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.TestValue;
import org.junit.Test;

public class BlueKeyValuePairTest {

	@Test
	public void test() {
		BlueKey key = new TimeFrameKey(1, 10, 15);
		TestValue value = new TestValue("Joe", 3);
		BlueKeyValuePair<TestValue> keyValuePair = new BlueKeyValuePair<TestValue>(key, value);
		assertEquals(value, keyValuePair.getValue());
		assertEquals(key, keyValuePair.getKey());
	}

}
