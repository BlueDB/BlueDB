package org.bluedb.api.exceptions;

import org.junit.Test;
import static org.junit.Assert.*;

public class UncheckedBlueDbExceptionTest {

	@Test
	public void test_uncheckedBlueDbException_canCreateSuccessfully() {
		// Arrange
		BlueDbException wrappedException = new BlueDbException("Fake reason");
		// Act
		UncheckedBlueDbException actual = new UncheckedBlueDbException(wrappedException);
		UncheckedBlueDbException actualWithReason = new UncheckedBlueDbException("Custom reason", wrappedException);
		// No assert needed, pass if no errors occur
	}

	@Test
	public void test_getCause_returnsWrappedException() {
		// Arrange
		BlueDbException expected = new BlueDbException("Fake reason");
		// Act
		UncheckedBlueDbException ex = new UncheckedBlueDbException("Custom reason", expected);
		BlueDbException actual = ex.getCause();
		// Assert
		assertEquals(expected, actual);
	}

}
