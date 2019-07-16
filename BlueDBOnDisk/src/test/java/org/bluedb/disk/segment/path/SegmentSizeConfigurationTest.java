package org.bluedb.disk.segment.path;

import static java.util.Arrays.asList;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_HOUR;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_MILLI;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_MINUTE;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.List;

import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.junit.Test;

public class SegmentSizeConfigurationTest {
	
	@Test
	public void testInvalidConstructorParameters() {
		List<Long> folderSizes = asList(ONE_HOUR, 	24L, 30L, 12L);
		List<Long> rollups = asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR);
		
		try {
			new SegmentSizeConfiguration(TimeKey.class, null, rollups);
			fail();
		} catch (NullPointerException e) { }

		try {
			new SegmentSizeConfiguration(TimeKey.class, folderSizes, null);
			fail();
		} catch (NullPointerException e) { }
	}

	@Test
	public void testEquals() {
		Class<TimeKey> keyType1 = TimeKey.class;
		Class<LongKey> keyType2 = LongKey.class;
		List<Long> folderSizes1 = asList(ONE_HOUR, 	24L, 30L, 12L);
		List<Long> folderSizes2 = asList(ONE_HOUR, ONE_MINUTE, 	24L, 30L, 12L);
		List<Long> folderSizes3 = asList(ONE_HOUR+1, ONE_MINUTE, 	24L, 30L, 12L);
		List<Long> rollups1 = asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR);
		List<Long> rollups2 = asList(ONE_MILLI, SIX_SECONDS, ONE_MINUTE, ONE_HOUR);
		
		SegmentSizeConfiguration config1 = new SegmentSizeConfiguration(keyType1, folderSizes1, rollups1);
		SegmentSizeConfiguration config2 = new SegmentSizeConfiguration(null, folderSizes1, rollups1);
		SegmentSizeConfiguration config3 = new SegmentSizeConfiguration(keyType2, folderSizes1, rollups1);
		SegmentSizeConfiguration config4 = new SegmentSizeConfiguration(keyType1, folderSizes2, rollups1);
		SegmentSizeConfiguration config5 = new SegmentSizeConfiguration(keyType1, folderSizes1, rollups2);
		SegmentSizeConfiguration config6 = new SegmentSizeConfiguration(keyType1, folderSizes3, rollups1);
		SegmentSizeConfiguration config7 = new SegmentSizeConfiguration(keyType1, folderSizes1, rollups1);
		
		assertNotEquals(config1, null);
		assertNotEquals(config1, "");
		assertNotEquals(config1, config2);
		assertNotEquals(config1.hashCode(), config2.hashCode());
		
		assertNotEquals(config1, config3);
		assertNotEquals(config1.hashCode(), config3.hashCode());
		
		assertNotEquals(config1, config4);
		assertNotEquals(config1.hashCode(), config4.hashCode());
		
		assertNotEquals(config1, config5);
		assertNotEquals(config1.hashCode(), config5.hashCode());
		
		assertNotEquals(config1, config6);
		assertNotEquals(config1.hashCode(), config6.hashCode());
		
		assertEquals(config1, config1);
		assertEquals(config1.hashCode(), config1.hashCode());
		
		assertEquals(config1, config7);
		assertEquals(config1.hashCode(), config7.hashCode());
	}

	@Test
	public void testHashcode() {
		Class<TimeKey> keyType1 = TimeKey.class;
		Class<LongKey> keyType2 = LongKey.class;
		List<Long> folderSizes1 = asList(ONE_HOUR, 	24L, 30L, 12L);
		List<Long> folderSizes2 = asList(ONE_HOUR, ONE_MINUTE, 	24L, 30L, 12L);
		List<Long> folderSizes3 = asList(ONE_HOUR+1, ONE_MINUTE, 	24L, 30L, 12L);
		List<Long> rollups1 = asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR);
		List<Long> rollups2 = asList(ONE_MILLI, SIX_SECONDS, ONE_MINUTE, ONE_HOUR);
		
		SegmentSizeConfiguration config1 = new SegmentSizeConfiguration(keyType1, folderSizes1, rollups1);
		SegmentSizeConfiguration config2 = new SegmentSizeConfiguration(null, folderSizes1, rollups1);
		SegmentSizeConfiguration config3 = new SegmentSizeConfiguration(keyType2, folderSizes1, rollups1);
		SegmentSizeConfiguration config4 = new SegmentSizeConfiguration(keyType1, folderSizes2, rollups1);
		SegmentSizeConfiguration config5 = new SegmentSizeConfiguration(keyType1, folderSizes1, rollups2);
		SegmentSizeConfiguration config6 = new SegmentSizeConfiguration(keyType1, folderSizes3, rollups1);
		SegmentSizeConfiguration config7 = new SegmentSizeConfiguration(keyType1, folderSizes1, rollups1);
		
		assertNotEquals(config1.hashCode(), config2.hashCode());
		assertNotEquals(config1.hashCode(), config3.hashCode());
		assertNotEquals(config1.hashCode(), config4.hashCode());
		assertNotEquals(config1.hashCode(), config5.hashCode());
		assertNotEquals(config1.hashCode(), config6);
		assertEquals(config1, config1);
		assertEquals(config1, config7);
	}

}
