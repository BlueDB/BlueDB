package io.bluedb.disk.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.bluedb.disk.file.FileManager;
import io.bluedb.disk.segment.Range;
import junit.framework.TestCase;

public class FileUtilsTest extends TestCase {

	private Path testPath;

	@Override
	protected void setUp() throws Exception {
		testPath = Files.createTempDirectory(this.getClass().getSimpleName());
	}

	@Override
	protected void tearDown() throws Exception {
		testPath.toFile().delete();
	}

	@Test
	public void test_constructor() {
		new FileUtils();  // just to get test coverage to 100%
	}

	@Test
	public void test_getRangeFileName() {
		assertEquals("0_1", FileUtils.getRangeFileName(0, 2));  // test zero
		assertEquals("2_3", FileUtils.getRangeFileName(2, 2));  // test next doesn't overlap
		assertEquals("4_5", FileUtils.getRangeFileName(5, 2));  // test greater than a multiple
		assertEquals("0_41", FileUtils.getRangeFileName(41, 42));  // test equal to a multiple
		assertEquals("42_83", FileUtils.getRangeFileName(42, 42));  // test equal to a multiple
		assertEquals("42_83", FileUtils.getRangeFileName(42, 42));  // test equal to a multiple
		assertEquals("-2_-1", FileUtils.getRangeFileName(-1, 2));  // test zero
		
		String maxLongFileName = FileUtils.getRangeFileName(Long.MAX_VALUE, 100);
		Range maxLongRange = Range.fromUnderscoreDelmimitedString(maxLongFileName);
		assertTrue(maxLongRange.getEnd() > maxLongRange.getStart());
		assertEquals(Long.MAX_VALUE, maxLongRange.getEnd());

		String minLongFileName = FileUtils.getRangeFileName(Long.MIN_VALUE, 100);
		Range minLongRange = Range.fromUnderscoreDelmimitedString(minLongFileName);
		assertTrue(minLongRange.getEnd() > minLongRange.getStart());
		assertEquals(Long.MIN_VALUE, minLongRange.getStart());
	}

	@Test
	public void test_doesfileNameRangeOverlap() {
		File _x_to_1 = Paths.get("1_x").toFile();
		File _1_to_x = Paths.get("1_x").toFile();
		File _1_to_3 = Paths.get("1_3").toFile();
		File _1 = Paths.get("1_").toFile();
		File _1_to_3_in_subfolder = Paths.get("whatever", "1_3").toFile();
		assertFalse(FileUtils.doesfileNameRangeOverlap(_1, 0, 10));
		assertFalse(FileUtils.doesfileNameRangeOverlap(_x_to_1, 0, 10));
		assertFalse(FileUtils.doesfileNameRangeOverlap(_1_to_x, 0, 10));
		assertTrue(FileUtils.doesfileNameRangeOverlap(_1_to_3, 0, 10));
		assertTrue(FileUtils.doesfileNameRangeOverlap(_1_to_3_in_subfolder, 0, 10));
		assertFalse(FileUtils.doesfileNameRangeOverlap(_1_to_3, 0, 0));  // above range
		assertTrue(FileUtils.doesfileNameRangeOverlap(_1_to_3, 0, 1));  // top of range
		assertTrue(FileUtils.doesfileNameRangeOverlap(_1_to_3, 2, 2));  // point
		assertTrue(FileUtils.doesfileNameRangeOverlap(_1_to_3, 0, 5));  // middle of range
		assertTrue(FileUtils.doesfileNameRangeOverlap(_1_to_3, 3, 4));  // bottom of range
		assertFalse(FileUtils.doesfileNameRangeOverlap(_1_to_3, 4, 5));  // below range
	}

	@Test
	public void test_isFileNameRangeEnclosed() {
		File _x_to_1 = Paths.get("1_x").toFile();
		File _1_to_x = Paths.get("1_x").toFile();
		File _1_to_3 = Paths.get("1_3").toFile();
		File _1 = Paths.get("1_").toFile();
		File _1_to_3_in_subfolder = Paths.get("whatever", "1_3").toFile();
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1, 0, 10));
		assertFalse(FileUtils.isFileNameRangeEnclosed(_x_to_1, 0, 10));
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1_to_x, 0, 10));
		assertTrue(FileUtils.isFileNameRangeEnclosed(_1_to_3, 0, 10));
		assertTrue(FileUtils.isFileNameRangeEnclosed(_1_to_3_in_subfolder, 0, 10));
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1_to_3, 0, 0));  // above range
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1_to_3, 0, 1));  // top of range
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1_to_3, 2, 2));  // point
		assertTrue(FileUtils.isFileNameRangeEnclosed(_1_to_3, 0, 5));  // middle of range
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1_to_3, 3, 4));  // bottom of range
		assertFalse(FileUtils.isFileNameRangeEnclosed(_1_to_3, 4, 5));  // below range
	}

	@Test
	public void test_getOrderedFilesInRange() throws Exception {
		File _12_13 = Paths.get(getPath().toString(), "12_13").toFile();
		File _12_15 = Paths.get(getPath().toString(), "12_15").toFile();
		File _2_3 = Paths.get(getPath().toString(), "2_3").toFile();
		File _100_101 = Paths.get(getPath().toString(), "100_101").toFile();
		List<File> expected = Arrays.asList(_2_3, _12_13, _12_15);

		FileManager.ensureFileExists(_12_13.toPath());
		FileManager.ensureFileExists(_12_15.toPath());
		FileManager.ensureFileExists(_2_3.toPath());
		FileManager.ensureFileExists(_100_101.toPath());
		Range timeRange = new Range(0, 20);
		assertEquals(expected, FileUtils.getOrderedFilesInRange(getPath(), timeRange));
	}

	@Test
	public void test_sortByRange() {
		File _12_13 = Paths.get(getPath().toString(), "12_13").toFile();
		File _12_15 = Paths.get(getPath().toString(), "12_15").toFile();
		File _2_3 = Paths.get(getPath().toString(), "2_3").toFile();
		List<File> unsorted = Arrays.asList(_12_15, _2_3, _12_13);
		List<File> sorted = Arrays.asList(_2_3, _12_13, _12_15);

		assertFalse(unsorted.equals(sorted));
		FileUtils.sortByRange(unsorted);
		assertTrue(unsorted.equals(sorted));
	}

	
	
	private Path getPath() {
		return testPath;
	}
}
