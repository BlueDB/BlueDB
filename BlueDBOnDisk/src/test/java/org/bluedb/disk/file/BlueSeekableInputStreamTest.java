package org.bluedb.disk.file;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

import org.bluedb.api.exceptions.BlueDbException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BlueSeekableInputStreamTest {
	
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
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(new File("non-existent-file.boblablahslawblog"))) {
			fail();
		} catch(BlueDbException e) {
			//expected to fail if the file doesn't exist
		}
	}
	
	@Test
	public void test_getDescription() throws BlueDbException, IOException {
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			assertEquals("file " + tmpFilePath.toFile(), inputStream.getDescription());
		}
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath.toFile())) {
			assertEquals("file " + tmpFilePath.toFile(), inputStream.getDescription());
		}
		
		RandomAccessFile mockedRandomAccessFile = Mockito.mock(RandomAccessFile.class);
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile)) {
			assertEquals("file " + mockedRandomAccessFile, inputStream.getDescription());
		}
	}
	
	@Test
	public void test_getGetTotalBytesInStream() throws BlueDbException, IOException {
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			assertEquals(0, inputStream.getTotalBytesInStream());
		}
		
		try(FileOutputStream fos = new FileOutputStream(tmpFilePath.toFile())) {
			fos.write(new byte[100]);
		}
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			assertEquals(100, inputStream.getTotalBytesInStream());
		}
		
		try(RandomAccessFile raf = new RandomAccessFile(tmpFilePath.toFile(), "r");
			BlueSeekableInputStream inputStream = new BlueSeekableInputStream(raf);) {
			assertEquals(100, inputStream.getTotalBytesInStream());
		}
	}

	@Test
	public void test_readNextByteAsInt() throws IOException, BlueDbException {
		try(DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(tmpFilePath.toFile()))) {
			dataOutputStream.write(5);
			dataOutputStream.write(213);
			dataOutputStream.write(40);
		}
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			assertEquals(5, inputStream.readNextByteAsInt());
			assertEquals(213, inputStream.readNextByteAsInt());
			assertEquals(40, inputStream.readNextByteAsInt());
		}
		
		RandomAccessFile mockedRandomAccessFile = Mockito.mock(RandomAccessFile.class);
		Mockito.when(mockedRandomAccessFile.read()).thenThrow(new RuntimeException("Bad Error"));
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile)) {
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
		try(DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(tmpFilePath.toFile()))) {
			dataOutputStream.write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
		}
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			byte[] buffer = new byte[4];
			
			assertEquals(4, inputStream.readBytes(buffer, 0, 4));
			assertArrayEquals(new byte[] { 0, 1, 2, 3}, buffer);
			
			assertEquals(4, inputStream.readBytes(buffer, 0, 4));
			assertArrayEquals(new byte[] { 4, 5, 6, 7}, buffer);
			
			assertEquals(2, inputStream.readBytes(buffer, 0, 4));
			assertArrayEquals(new byte[] { 8, 9, 6, 7}, buffer);
			
			assertEquals(-1, inputStream.readBytes(buffer, 0, 4));
		}
		
		RandomAccessFile mockedRandomAccessFile = Mockito.mock(RandomAccessFile.class);
		Mockito.when(mockedRandomAccessFile.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new RuntimeException("Bad Error"));
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile)) {
			try {
				inputStream.readBytes(new byte[5], 0, 5);
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}

	@Test
	public void test_readFully() throws IOException, BlueDbException {
		try(DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(tmpFilePath.toFile()))) {
			dataOutputStream.write(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
		}
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			byte[] buffer = new byte[4];
			
			inputStream.readFully(buffer, 0, 4);
			assertArrayEquals(new byte[] { 0, 1, 2, 3}, buffer);
			
			inputStream.readFully(buffer, 0, 4);
			assertArrayEquals(new byte[] { 4, 5, 6, 7}, buffer);
			
			try {
				inputStream.readFully(buffer, 0, 4);
				fail();
			} catch(BlueDbException e) {
				//Expected since there are only 2 bytes left to read but we asked to read 4
			}
		}
		
		RandomAccessFile mockedRandomAccessFile = Mockito.mock(RandomAccessFile.class);
		Mockito.when(mockedRandomAccessFile.read(Mockito.any(), Mockito.anyInt(), Mockito.anyInt())).thenThrow(new RuntimeException("Bad Error"));
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile)) {
			try {
				inputStream.readFully(new byte[5], 0, 5);
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}

	@Test
	public void test_markAndReset() throws IOException, BlueDbException {
		try(DataOutputStream dataOutputStream = new DataOutputStream(new FileOutputStream(tmpFilePath.toFile()))) {
			dataOutputStream.write(5);
			dataOutputStream.write(213);
			dataOutputStream.write(40);
		}
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(tmpFilePath)) {
			assertEquals(0, inputStream.getCursorPosition());
			assertEquals(5, inputStream.readNextByteAsInt());
			assertEquals(1, inputStream.getCursorPosition());
			
			inputStream.mark(Integer.MAX_VALUE);
			assertEquals(213, inputStream.readNextByteAsInt());
			assertEquals(40, inputStream.readNextByteAsInt());
			
			inputStream.resetToLastMark();
			assertEquals(213, inputStream.readNextByteAsInt());
			assertEquals(40, inputStream.readNextByteAsInt());
			
			inputStream.setCursorPosition(2);
			assertEquals(40, inputStream.readNextByteAsInt());
		}
		
		RandomAccessFile mockedRandomAccessFile = Mockito.mock(RandomAccessFile.class);
		Mockito.when(mockedRandomAccessFile.getFilePointer()).thenThrow(new RuntimeException("Bad Error"));
		Mockito.doThrow(new RuntimeException("Bad Error")).when(mockedRandomAccessFile).seek(Mockito.anyLong());
		
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile)) {
			try {
				inputStream.getCursorPosition();
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
			
			try {
				inputStream.mark(Integer.MAX_VALUE);
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
			
			try {
				inputStream.resetToLastMark();
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
			
			try {
				inputStream.setCursorPosition(5);
				fail();
			} catch(BlueDbException e) {
				//Expected
			}
		}
	}
	
	@Test
	public void test_close() throws IOException, BlueDbException {
		RandomAccessFile mockedRandomAccessFile = Mockito.mock(RandomAccessFile.class);
		try(BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile)) {
			
		}
		Mockito.verify(mockedRandomAccessFile, times(1)).close();
		
		Mockito.doThrow(new IOException("Bad News")).when(mockedRandomAccessFile).close();
		BlueSeekableInputStream inputStream = new BlueSeekableInputStream(mockedRandomAccessFile);
		inputStream.close(); //Shouldn't throw an exception
	}

}
