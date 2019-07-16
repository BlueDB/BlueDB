package org.bluedb.disk.segment.path;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class SegmentPathTimeUnitsTest {

	@Test
	public void coverDefaultConstructorForStaticUtilClass() {
		new SegmentPathTimeUnits();
	}
	
	@Test
	public void testValues() {
		assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MILLISECONDS), SegmentPathTimeUnits.ONE_MILLI);
		assertEquals(TimeUnit.MILLISECONDS.convert(6, TimeUnit.SECONDS), SegmentPathTimeUnits.SIX_SECONDS);
		assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES), SegmentPathTimeUnits.ONE_MINUTE);
		
		assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS), SegmentPathTimeUnits.ONE_HOUR);
		assertEquals(TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS), SegmentPathTimeUnits.TWO_HOURS);
		assertEquals(TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS), SegmentPathTimeUnits.SIX_HOURS);
		assertEquals(TimeUnit.MILLISECONDS.convert(12, TimeUnit.HOURS), SegmentPathTimeUnits.TWELVE_HOURS);
		
		assertEquals(TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS), SegmentPathTimeUnits.ONE_DAY);
		assertEquals(TimeUnit.MILLISECONDS.convert(5, TimeUnit.DAYS), SegmentPathTimeUnits.FIVE_DAYS);
		assertEquals(TimeUnit.MILLISECONDS.convert(15, TimeUnit.DAYS), SegmentPathTimeUnits.FIFTEEN_DAYS);
		assertEquals(TimeUnit.MILLISECONDS.convert(30, TimeUnit.DAYS), SegmentPathTimeUnits.ONE_MONTH);
		assertEquals(TimeUnit.MILLISECONDS.convert(90, TimeUnit.DAYS), SegmentPathTimeUnits.THREE_MONTHS);
		assertEquals(TimeUnit.MILLISECONDS.convert(180, TimeUnit.DAYS), SegmentPathTimeUnits.SIX_MONTHS);
	}

}
