package org.bluedb.disk.file;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class BlueDataInputStreamTest {
	
	private Path tmpFilePath;

	@Before
	public void setup() throws Exception {
		tmpFilePath = Files.createTempFile(getClass().getSimpleName(), null);
		tmpFilePath.toFile().deleteOnExit();
	}
	
	@After
	public void after() throws IOException {
		tmpFilePath.toFile().delete();
	}
	
	@Test
	public void test_constructorException() throws BlueDbException, IOException {
		try(BlueDataInputStream inputStream = new BlueDataInputStream(new File("non-existent-file.boblablahslawblog"))) {
			fail();
		} catch(BlueDbException e) {
			//expected to fail if the file doesn't exist
		}
	}
	
	@Test
	public void test_getDescription() throws BlueDbException, IOException {
		try(BlueDataInputStream inputStream = new BlueDataInputStream(tmpFilePath)) {
			assertEquals("file " + tmpFilePath.toFile(), inputStream.getDescription());
		}
		
		try(BlueDataInputStream inputStream = new BlueDataInputStream(tmpFilePath.toFile())) {
			assertEquals("file " + tmpFilePath.toFile(), inputStream.getDescription());
		}
		
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			assertEquals("input stream " + mockedInputStream, inputStream.getDescription());
		}
	}
	
	@Test
	public void test_readNextByteAsInt() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		Mockito.when(mockedInputStream.read()).thenReturn(1, 200, 76, 17);
		
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			assertEquals(1, inputStream.readNextByteAsInt());
			assertEquals(200, inputStream.readNextByteAsInt());
			assertEquals(76, inputStream.readNextByteAsInt());
			assertEquals(17, inputStream.readNextByteAsInt());
			assertEquals("input stream " + mockedInputStream, inputStream.getDescription());
			
			Mockito.reset(mockedInputStream);
			Mockito.when(mockedInputStream.read()).thenThrow(new RuntimeException("Bad Error"));
			try {
				inputStream.readNextByteAsInt();
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test
	public void test_readNextFourBytesAsInt() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		Mockito.when(mockedInputStream.read()).thenReturn(1, 200, 76, 17, 14);
		
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			assertEquals(29903889, (int) inputStream.readNextFourBytesAsInt());
			assertEquals(14, inputStream.readNextByteAsInt());
			
			Mockito.reset(mockedInputStream);
			Mockito.when(mockedInputStream.read()).thenReturn(1, 200, 76, -4); // -4 is invalid
			assertEquals(null, (Integer) inputStream.readNextFourBytesAsInt());
			
			Mockito.reset(mockedInputStream);
			Mockito.when(mockedInputStream.read()).thenThrow(new RuntimeException("Bad Error"));
			try {
				inputStream.readNextByteAsInt();
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test 
	public void test_readBytes() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		Mockito.when(mockedInputStream.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenAnswer(new ReadBytesTestImpl());
		
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			byte[] buffer = new byte[] { (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0 }; 
			assertArrayEquals(buffer, new byte[] { (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0 });
			inputStream.readBytes(buffer, 3, 5);
			assertArrayEquals(buffer, new byte[] { (byte)0, (byte)0, (byte)0, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)0, (byte)0, (byte)0 });
			
			Mockito.reset(mockedInputStream);
			Mockito.when(mockedInputStream.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new RuntimeException("Bad Error"));
			try {
				inputStream.readBytes(buffer, 3, 5);
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test 
	public void test_readAllRemainingBytes() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		Mockito.when(mockedInputStream.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenAnswer(new ReadBytesTestImpl());
		
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			byte[] bytes = inputStream.readAllRemainingBytes();
			int expectedBytesLength = ((ReadBytesTestImpl.TIMES_TO_RETURN_BYTES-1) * 1024) + ReadBytesTestImpl.BYTES_TO_RETURN_ON_LAST_INVOCATION;
			assertEquals(expectedBytesLength, bytes.length);
			
			Mockito.reset(mockedInputStream);
			Mockito.when(mockedInputStream.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new RuntimeException("Bad Error"));
			try {
				inputStream.readAllRemainingBytes();
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test 
	public void test_readFully() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		Mockito.when(mockedInputStream.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenAnswer(invocation -> {
			byte[] buffer = (byte[]) invocation.getArguments()[0];
			int offset = (int) invocation.getArguments()[1];
			int length = (int) invocation.getArguments()[2];
			int bytesWritten = 0;
			for(int i = offset; i < buffer.length && bytesWritten < length; i++) {
				buffer[i] = (byte) (i % 255);
				bytesWritten++;
			}
			return bytesWritten;
		});
		
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			byte[] buffer = new byte[] { (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0 }; 
			assertArrayEquals(buffer, new byte[] { (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0, (byte)0 });
			inputStream.readFully(buffer, 3, 5);
			assertArrayEquals(buffer, new byte[] { (byte)0, (byte)0, (byte)0, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)0, (byte)0, (byte)0 });
			
			Mockito.reset(mockedInputStream);
			Mockito.when(mockedInputStream.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new RuntimeException("Bad Error"));
			try {
				inputStream.readFully(buffer, 3, 5);
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test
	public void test_mark() throws IOException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			inputStream.mark(11);
			Mockito.verify(mockedInputStream, times(1)).mark(11);
		}
	}
	
	@Test
	public void test_reset() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			inputStream.resetToLastMark();
			Mockito.verify(mockedInputStream, times(1)).reset();
			
			Mockito.reset(mockedInputStream);
			Mockito.doThrow(new RuntimeException("Bad Error")).when(mockedInputStream).reset();
			try {
				inputStream.resetToLastMark();
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test
	public void test_close() throws IOException, BlueDbException {
		InputStream mockedInputStream = Mockito.mock(InputStream.class);
		try(BlueDataInputStream inputStream = new BlueDataInputStream(mockedInputStream)) {
			
		}
		Mockito.verify(mockedInputStream, times(1)).close();
	}
	
	private static class ReadBytesTestImpl implements Answer<Integer> {
		private static final int TIMES_TO_RETURN_BYTES = 3;
		private static final int BYTES_TO_RETURN_ON_LAST_INVOCATION = 50;
		private int invocationCount = 0;
		
		@Override
		public Integer answer(InvocationOnMock invocation) throws Throwable {
			invocationCount++;
			byte[] buffer = (byte[]) invocation.getArguments()[0];
			int offset = (int) invocation.getArguments()[1];
			int length = (int) invocation.getArguments()[2];

			if(isEmpty()) {
				return -1;
			}
			
			int bytesToWrite = calculateBytesToWrite(buffer, length);
			
			int bytesWritten = 0;
			for(int i = offset; i < buffer.length && bytesWritten < length && bytesWritten < bytesToWrite; i++) {
				buffer[i] = (byte) (i % 255);
				bytesWritten++;
			}
			return bytesWritten;
		}

		private int calculateBytesToWrite(byte[] buffer, int length) {
			int bytesToWrite = Math.min(buffer.length, length);
			if(isLastTimeReturningBytes()) {
				bytesToWrite = BYTES_TO_RETURN_ON_LAST_INVOCATION;
			}
			return bytesToWrite;
		}

		private boolean isEmpty() {
			return invocationCount > TIMES_TO_RETURN_BYTES;
		}

		private boolean isLastTimeReturningBytes() {
			return invocationCount == TIMES_TO_RETURN_BYTES;
		}
	}
}
