package org.bluedb.disk.recovery;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.bluedb.api.exceptions.BlueDbException;
import junit.framework.TestCase;

public class TimeStampedFileTest extends TestCase {

	@Test
	public void test_extractTimestamp() {
		File _123 = Paths.get("123.whatever").toFile();
		File invalid = Paths.get("123_xyz").toFile();
		File _negative123 = Paths.get("-123.whatever").toFile();
		try {
			assertEquals(123, TimeStampedFile.extractTimestamp(_123).longValue());
			assertEquals(-123, TimeStampedFile.extractTimestamp(_negative123).longValue());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		try {
			TimeStampedFile.extractTimestamp(invalid);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_extractRecoverableId() {
		File _123 = Paths.get("124.123.whatever").toFile();
		File invalid = Paths.get("124_123_xyz").toFile();
		File _negative123 = Paths.get("-124.-123.whatever").toFile();
		assertEquals(123, TimeStampedFile.extractRecoverableId(_123).longValue());
		assertEquals(-123, TimeStampedFile.extractRecoverableId(_negative123).longValue());
		assertEquals(Long.valueOf(0), TimeStampedFile.extractRecoverableId(invalid));
	}

	@Test
	public void test_compareTo() throws Exception {
		File file_time1id1 = Paths.get("1.1.chg").toFile();
		File file_time1id1copy = Paths.get("1.1.chg").toFile();
		File file_time1id2 = Paths.get("1.2.chg").toFile();
		File file_time2id1 = Paths.get("2.1.chg").toFile();
		TimeStampedFile time1id1 = new TimeStampedFile(file_time1id1);
		TimeStampedFile time1id1copy = new TimeStampedFile(file_time1id1copy);
		TimeStampedFile time1id2 = new TimeStampedFile(file_time1id2);
		TimeStampedFile time2id1 = new TimeStampedFile(file_time2id1);
		
		assertTrue(time1id1.compareTo(time1id1) == 0);
		assertTrue(time1id1.compareTo(time1id1copy) == 0);
		assertTrue(time1id1.compareTo(time1id2) < 0);
		assertTrue(time1id2.compareTo(time1id1) > 0);
		assertTrue(time1id2.compareTo(time2id1) < 0);

		List<TimeStampedFile> recoverables = Arrays.asList(time2id1, time1id1, time1id2);
		List<TimeStampedFile> sorted = Arrays.asList(time1id1, time1id2, time2id1);
		Collections.sort(recoverables);
		assertEquals(sorted, recoverables);
	}
}
