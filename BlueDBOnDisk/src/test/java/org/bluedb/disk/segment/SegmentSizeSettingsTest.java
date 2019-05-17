package org.bluedb.disk.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.junit.Test;

public class SegmentSizeSettingsTest {

	@Test
	public void testGetDefaultSettingsFor() throws Exception {
		assertEquals(SegmentSizeSettings.TIME_1_HOUR, SegmentSizeSettings.getDefaultSettingsFor(TimeKey.class));
		assertEquals(SegmentSizeSettings.TIME_1_HOUR, SegmentSizeSettings.getDefaultSettingsFor(TimeFrameKey.class));
		assertEquals(SegmentSizeSettings.LONG_DEFAULT, SegmentSizeSettings.getDefaultSettingsFor(LongKey.class));
		assertEquals(SegmentSizeSettings.INT_DEFAULT, SegmentSizeSettings.getDefaultSettingsFor(IntegerKey.class));
		assertEquals(SegmentSizeSettings.HASH_DEFAULT, SegmentSizeSettings.getDefaultSettingsFor(StringKey.class));
		assertEquals(SegmentSizeSettings.HASH_DEFAULT, SegmentSizeSettings.getDefaultSettingsFor(UUIDKey.class));
	}

	@Test
	public void testGetDefaultSegmentSizeFor() throws Exception {
		assertEquals(SegmentSizeSettings.TIME_1_HOUR.getSegmentSize(), SegmentSizeSettings.getDefaultSegmentSizeFor(TimeKey.class));
		assertEquals(SegmentSizeSettings.TIME_1_HOUR.getSegmentSize(), SegmentSizeSettings.getDefaultSegmentSizeFor(TimeFrameKey.class));
		assertEquals(SegmentSizeSettings.LONG_DEFAULT.getSegmentSize(), SegmentSizeSettings.getDefaultSegmentSizeFor(LongKey.class));
		assertEquals(SegmentSizeSettings.INT_DEFAULT.getSegmentSize(), SegmentSizeSettings.getDefaultSegmentSizeFor(IntegerKey.class));
		assertEquals(SegmentSizeSettings.HASH_DEFAULT.getSegmentSize(), SegmentSizeSettings.getDefaultSegmentSizeFor(StringKey.class));
		assertEquals(SegmentSizeSettings.HASH_DEFAULT.getSegmentSize(), SegmentSizeSettings.getDefaultSegmentSizeFor(UUIDKey.class));
	}

	@Test
	public void testGetSettings() throws Exception{
		assertEquals(SegmentSizeSettings.TIME_1_HOUR, SegmentSizeSettings.getSettings(TimeKey.class, SegmentSizeSettings.TIME_1_HOUR.getSegmentSize()));
		assertEquals(SegmentSizeSettings.TIME_1_DAY, SegmentSizeSettings.getSettings(TimeKey.class, SegmentSizeSettings.TIME_1_DAY.getSegmentSize()));
		assertEquals(SegmentSizeSettings.TIME_1_HOUR, SegmentSizeSettings.getSettings(TimeFrameKey.class, SegmentSizeSettings.TIME_1_HOUR.getSegmentSize()));
		assertEquals(SegmentSizeSettings.TIME_1_DAY, SegmentSizeSettings.getSettings(TimeFrameKey.class, SegmentSizeSettings.TIME_1_DAY.getSegmentSize()));

		assertEquals(SegmentSizeSettings.LONG_DEFAULT, SegmentSizeSettings.getSettings(LongKey.class, SegmentSizeSettings.LONG_DEFAULT.getSegmentSize()));
		assertEquals(SegmentSizeSettings.INT_DEFAULT, SegmentSizeSettings.getSettings(IntegerKey.class, SegmentSizeSettings.INT_DEFAULT.getSegmentSize()));
		assertEquals(SegmentSizeSettings.HASH_DEFAULT, SegmentSizeSettings.getSettings(StringKey.class, SegmentSizeSettings.HASH_DEFAULT.getSegmentSize()));
		assertEquals(SegmentSizeSettings.HASH_DEFAULT, SegmentSizeSettings.getSettings(UUIDKey.class, SegmentSizeSettings.HASH_DEFAULT.getSegmentSize()));

		try {
			SegmentSizeSettings.getSettings(TimeFrameKey.class, SegmentSizeSettings.HASH_DEFAULT.getSegmentSize());
			fail();
		} catch (Exception e) {}

	}

}
