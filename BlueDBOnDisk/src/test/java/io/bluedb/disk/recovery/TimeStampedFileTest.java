package io.bluedb.disk.recovery;

import static org.junit.Assert.*;
import java.io.File;
import java.nio.file.Paths;
import org.junit.Test;

public class TimeStampedFileTest {

	@Test
	public void test_extractTimestamp() {
		File _123 = Paths.get("123.whatever").toFile();
		File invalid = Paths.get("123_xyz").toFile();
		File _negative123 = Paths.get("-123.whatever").toFile();
		assertEquals(123, TimeStampedFile.extractTimestamp(_123).longValue());
		assertNull(TimeStampedFile.extractTimestamp(invalid));
		assertEquals(-123, TimeStampedFile.extractTimestamp(_negative123).longValue());
	}
}
