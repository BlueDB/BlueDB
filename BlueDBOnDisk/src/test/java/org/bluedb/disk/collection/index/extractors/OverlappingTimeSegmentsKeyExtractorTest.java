package org.bluedb.disk.collection.index.extractors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.UUID;

import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.segment.ReadableSegmentManager;
import org.bluedb.disk.segment.path.SegmentPathManager;
import org.junit.Test;
import org.mockito.Mockito;

public class OverlappingTimeSegmentsKeyExtractorTest {

	@Test
	public void test_keyType() {
		OverlappingTimeSegmentsKeyExtractor<TestValue> extractor = new OverlappingTimeSegmentsKeyExtractor<TestValue>();
		assertEquals(LongTimeKey.class, extractor.getType());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_extractKeys() {
		SegmentPathManager segPathManagerMock = Mockito.mock(SegmentPathManager.class);
		Mockito.doReturn(Arrays.asList(10L, 11L, 12L)).when(segPathManagerMock).getAllPossibleSegmentStartGroupingNumbers(Mockito.any());
		
		ReadableSegmentManager<TestValue> segManagerMock = (ReadableSegmentManager<TestValue>) Mockito.mock(ReadableSegmentManager.class);
		Mockito.doReturn(segPathManagerMock).when(segManagerMock).getPathManager();
		
		TimeFrameKey key = new TimeFrameKey(UUID.randomUUID(), 0, 1);
		OverlappingTimeSegmentsKeyExtractor<TestValue> extractor = new OverlappingTimeSegmentsKeyExtractor<TestValue>();
		assertEquals(Arrays.asList(new LongTimeKey(11), new LongTimeKey(12)), extractor.extractKeys(key, segManagerMock));
	}

	@Test
	public void test_extractKeys_passingValueInThrowsUnsupportedException() {
		OverlappingTimeSegmentsKeyExtractor<TestValue> extractor = new OverlappingTimeSegmentsKeyExtractor<TestValue>();
		try {
			extractor.extractKeys(new TestValue("Bob"));
			fail("This method should be unsupported by this extractor type");
		} catch(UnsupportedOperationException e) {
			//Expected
		}
	}

}