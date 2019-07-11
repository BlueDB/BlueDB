package org.bluedb.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.segment.SegmentSizeSetting;
import org.junit.Test;

public class SegmentSizeTest {
	@Test
	public void testUserSelectionMapping() throws BlueDbException {
		assertEquals(SegmentSizeSetting.TIME_1_HOUR, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_1_HOUR));
		assertEquals(SegmentSizeSetting.TIME_2_HOURS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_2_HOURS));
		assertEquals(SegmentSizeSetting.TIME_6_HOURS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_6_HOURS));
		assertEquals(SegmentSizeSetting.TIME_12_HOURS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_12_HOURS));
		
		assertEquals(SegmentSizeSetting.TIME_1_DAY, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_1_DAY));
		assertEquals(SegmentSizeSetting.TIME_5_DAYS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_5_DAYS));
		assertEquals(SegmentSizeSetting.TIME_15_DAYS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_15_DAYS));
		
		assertEquals(SegmentSizeSetting.TIME_1_MONTH, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_1_MONTH));
		assertEquals(SegmentSizeSetting.TIME_3_MONTHS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_3_MONTHS));
		assertEquals(SegmentSizeSetting.TIME_6_MONTHS, SegmentSizeSetting.fromUserSelection(SegmentSize.TIME_6_MONTHS));
		
		assertEquals(SegmentSizeSetting.INT_128, SegmentSizeSetting.fromUserSelection(SegmentSize.INT_128));
		assertEquals(SegmentSizeSetting.INT_256, SegmentSizeSetting.fromUserSelection(SegmentSize.INT_256));
		assertEquals(SegmentSizeSetting.INT_512, SegmentSizeSetting.fromUserSelection(SegmentSize.INT_512));
		assertEquals(SegmentSizeSetting.INT_1K, SegmentSizeSetting.fromUserSelection(SegmentSize.INT_1K));
		
		assertEquals(SegmentSizeSetting.LONG_128, SegmentSizeSetting.fromUserSelection(SegmentSize.LONG_128));
		assertEquals(SegmentSizeSetting.LONG_256, SegmentSizeSetting.fromUserSelection(SegmentSize.LONG_256));
		assertEquals(SegmentSizeSetting.LONG_512, SegmentSizeSetting.fromUserSelection(SegmentSize.LONG_512));
		assertEquals(SegmentSizeSetting.LONG_1K, SegmentSizeSetting.fromUserSelection(SegmentSize.LONG_1K));
		
		assertEquals(SegmentSizeSetting.HASH_256K, SegmentSizeSetting.fromUserSelection(SegmentSize.HASH_256K));
		assertEquals(SegmentSizeSetting.HASH_512K, SegmentSizeSetting.fromUserSelection(SegmentSize.HASH_512K));
		assertEquals(SegmentSizeSetting.HASH_1M, SegmentSizeSetting.fromUserSelection(SegmentSize.HASH_1M));
		assertEquals(SegmentSizeSetting.HASH_2M, SegmentSizeSetting.fromUserSelection(SegmentSize.HASH_2M));
		assertEquals(SegmentSizeSetting.HASH_4M, SegmentSizeSetting.fromUserSelection(SegmentSize.HASH_4M));
		
		try {
			SegmentSizeSetting.fromUserSelection(new SegmentSize<>("whatever"));
			fail();
		} catch(BlueDbException e) { }
	}
}
