package org.bluedb.api.index;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.UUID;

import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.IndexableTestValue;
import org.junit.Test;

public class UUIDIndexExtractorTest {

	@Test
	public void test() {
		IndexableTestValue testValue = new IndexableTestValue(UUID.randomUUID(), 0, 5, "Whatever", 27);
		
		UUIDIndexExtractor<IndexableTestValue> extractor = value -> Arrays.asList(value.getId());
		assertEquals(Arrays.asList(testValue.getUUIDKey()), extractor.extractKeys(testValue));
		
		UUIDIndexExtractor<IndexableTestValue> badExtractor = value -> null;
		assertEquals(Arrays.asList(), badExtractor.extractKeys(testValue));
		
		UUID extraUUID = UUID.randomUUID();
		UUIDIndexExtractor<IndexableTestValue> listExtractor = value -> Arrays.asList(value.getId(), extraUUID);
		assertEquals(Arrays.asList(testValue.getUUIDKey(), new UUIDKey(extraUUID)), listExtractor.extractKeys(testValue));
	}

}
