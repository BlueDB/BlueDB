package io.bluedb.disk.recovery;

import static org.junit.Assert.*;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Test;
import io.bluedb.api.exceptions.BlueDbException;

public class TimeStampedFileTest {

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
}
