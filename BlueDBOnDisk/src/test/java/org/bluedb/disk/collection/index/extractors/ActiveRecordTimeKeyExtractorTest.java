package org.bluedb.disk.collection.index.extractors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.bluedb.api.keys.ActiveTimeKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.junit.Test;

public class ActiveRecordTimeKeyExtractorTest {

	@Test
	public void test_keyType() {
		ActiveRecordTimeKeyExtractor<TestValue> extractor = new ActiveRecordTimeKeyExtractor<TestValue>();
		assertEquals(LongTimeKey.class, extractor.getType());
	}
	
	@Test
	public void test_extractKeys() {
		ActiveRecordTimeKeyExtractor<TestValue> extractor = new ActiveRecordTimeKeyExtractor<TestValue>();

		assertEquals(Arrays.asList(), extractor.extractKeys(null, null));
		assertEquals(Arrays.asList(), extractor.extractKeys(new TimeKey(1, 1), null));
		assertEquals(Arrays.asList(), extractor.extractKeys(new TimeFrameKey(1, 1, 10), null));
		assertEquals(Arrays.asList(), extractor.extractKeys(new StringKey("My String Key"), null));
		assertEquals(Arrays.asList(new LongTimeKey(1)), extractor.extractKeys(new ActiveTimeKey(1, 1), null));
		assertEquals(Arrays.asList(new LongTimeKey(100)), extractor.extractKeys(new ActiveTimeKey(1, 100), null));
	}

	@Test
	public void test_extractKeys_passingValueInThrowsUnsupportedException() {
		ActiveRecordTimeKeyExtractor<TestValue> extractor = new ActiveRecordTimeKeyExtractor<TestValue>();
		try {
			extractor.extractKeys(new TestValue("Bob"));
			fail("This method should be unsupported by this extractor type");
		} catch(UnsupportedOperationException e) {
			//Expected
		}
	}

}
