package org.bluedb.disk;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.junit.Test;

public class ByteUtilsTest {

	@Test
	public void test_testReplaceBytes() {
		String testString = "This is my super elaborate byte array to test with";
		testReplaceBytes(testString, "is", "was");
		testReplaceBytes(testString, " ", "%20");
		testReplaceBytes(testString, "r", "h");
		testReplaceBytes(testString, "r", "");
		testReplaceBytes(testString, "This is my super elaborate byte array to test with", "");
		testReplaceBytes(testString, "This is my super elaborate byte array to test with", "whole new array");
		testReplaceBytes(testString, "This ", "first");
		testReplaceBytes(testString, "with", "last");
		
		byte[] emptyBytes = new byte[0];
		
		assertEquals(null, ByteUtils.replaceAllBytes(null, null, null));
		assertArrayEquals(emptyBytes, ByteUtils.replaceAllBytes(emptyBytes, emptyBytes, emptyBytes));
		assertArrayEquals(testString.getBytes(), ByteUtils.replaceAllBytes(testString.getBytes(), null, "was".getBytes()));
		assertArrayEquals(testString.getBytes(), ByteUtils.replaceAllBytes(testString.getBytes(), emptyBytes, "was".getBytes()));
		assertArrayEquals(testString.getBytes(), ByteUtils.replaceAllBytes(testString.getBytes(), "is".getBytes(), null));
	}

	private void testReplaceBytes(String testString, String targetString, String replacement) {
		byte[] expected = testString.replaceAll(targetString, replacement).getBytes();
		byte[] actual = ByteUtils.replaceAllBytes(testString.getBytes(), targetString.getBytes(), replacement.getBytes());
		assertArrayEquals(expected, actual);
	}

	@Test
	public void test_testIndexOfThatIsntCoveredElsewhere() {
		byte[] testBytes = "This is my super elaborate byte array to test with".getBytes();
		
		byte[] emptyBytes = new byte[0];
		assertEquals(-1, ByteUtils.indexOf(testBytes, emptyBytes, 0));
	}

	@Test
	public void test_testReplaceClassPathBytesThatIsntCoveredElsewhere() {
		byte[] testBytes = "This is my super elaborate byte array to test with".getBytes();
		
		assertEquals(null, ByteUtils.replaceClassPathBytes(null, null, null));
		assertArrayEquals(testBytes, ByteUtils.replaceClassPathBytes(testBytes, null, "org.bluedb"));
		assertArrayEquals(testBytes, ByteUtils.replaceClassPathBytes(testBytes, "io.bluedb", null));
	}

	@Test
	public void test_testFstClassSizeMethods() {
		byte[] oneByteSize1 = new byte[] { (byte) 0, (byte) 39, (byte) 5 };
		byte[] oneByteSize2 = new byte[] { (byte) 0, (byte) 1, (byte) 39, (byte) 5 };
		byte[] oneByteSize3 = new byte[] { (byte) 90, (byte) 100, (byte) 120, (byte) 95, (byte) 114, (byte) 177, (byte) 0, (byte) 39, (byte) 5 };
		
		byte[] threeByteSize1 = new byte[] { (byte) 90, (byte) 100, (byte) 120, (byte) 95, (byte) 0, (byte) -128, (byte) 0xFF, (byte) 0x7F, (byte) 5 };
		
		byte[] fiveByteSize2 = new byte[] { (byte) 90, (byte) 100, (byte) 120, (byte) 95, (byte) 0, (byte) -127, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F, (byte) 5 };
		
		assertEquals(1, ByteUtils.determineNumberOfBytesUsedForFstClassSize(oneByteSize1, oneByteSize1.length-1));
		assertEquals(1, ByteUtils.determineNumberOfBytesUsedForFstClassSize(oneByteSize2, oneByteSize2.length-1));
		assertEquals(1, ByteUtils.determineNumberOfBytesUsedForFstClassSize(oneByteSize3, oneByteSize3.length-1));
		assertEquals(3, ByteUtils.determineNumberOfBytesUsedForFstClassSize(threeByteSize1, threeByteSize1.length-1));
		assertEquals(5, ByteUtils.determineNumberOfBytesUsedForFstClassSize(fiveByteSize2, fiveByteSize2.length-1));
		
		assertEquals(39, ByteUtils.readFstClassSize(oneByteSize1, 1));
		assertEquals(39, ByteUtils.readFstClassSize(oneByteSize2, 2));
		assertEquals(39, ByteUtils.readFstClassSize(oneByteSize3, 7));
		assertEquals(Short.MAX_VALUE, ByteUtils.readFstClassSize(threeByteSize1, 5));
		assertEquals(Integer.MAX_VALUE, ByteUtils.readFstClassSize(fiveByteSize2, 5));
		
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		ByteUtils.writeFstClassSize(buffer, 39);
		assertArrayEquals(Arrays.copyOfRange(oneByteSize1, 1, 2), buffer.toByteArray());
		
		buffer = new ByteArrayOutputStream();
		ByteUtils.writeFstClassSize(buffer, 39);
		assertArrayEquals(Arrays.copyOfRange(oneByteSize2, 2, 3), buffer.toByteArray());
		
		buffer = new ByteArrayOutputStream();
		ByteUtils.writeFstClassSize(buffer, 39);
		assertArrayEquals(Arrays.copyOfRange(oneByteSize3, 7, 8), buffer.toByteArray());
		
		buffer = new ByteArrayOutputStream();
		ByteUtils.writeFstClassSize(buffer, Short.MAX_VALUE);
		assertArrayEquals(Arrays.copyOfRange(threeByteSize1, 5, 8), buffer.toByteArray());
		
		buffer = new ByteArrayOutputStream();
		ByteUtils.writeFstClassSize(buffer, Integer.MAX_VALUE);
		assertArrayEquals(Arrays.copyOfRange(fiveByteSize2, 5, 10), buffer.toByteArray());
	}
}
