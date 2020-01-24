package org.bluedb.disk.file;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertArrayEquals;
import org.junit.Test;
import org.bluedb.TestUtils;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectInputTest extends TestCase {

	BlueSerializer serializer;
	ReadWriteFileManager fileManager;
	LockManager<Path> lockManager;
	Path testingFolderPath;
	Path targetFilePath;
	Path tempFilePath;

	@Override
	protected void setUp() throws Exception {
		testingFolderPath = Files.createTempDirectory(this.getClass().getSimpleName());
		targetFilePath = Paths.get(testingFolderPath.toString(), "BlueObjectOutputStreamTest.test_junk");
		tempFilePath = FileUtils.createTempFilePath(targetFilePath);
		serializer = new ThreadLocalFstSerializer(new Class[]{});
		fileManager = new ReadWriteFileManager(serializer);
		lockManager = fileManager.getLockManager();
	}

	@Override
	public void tearDown() throws Exception {
		targetFilePath.toFile().delete();
		tempFilePath.toFile().delete();
		Blutils.recursiveDelete(targetFilePath.toFile());
		Blutils.recursiveDelete(testingFolderPath.toFile());
	}



	@Test
	public void test_close() throws Exception {
		File emptyFile = createEmptyFile("your_cold_heart");
		
		try (BlueReadLock<Path> writeLock = lockManager.acquireReadLock(emptyFile.toPath())) {
			BlueObjectInput<TestValue> stream = fileManager.getBlueInputStream(writeLock);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(emptyFile.exists());
		}
	}

	@Test
	public void test_openDataInputStream() {
		File nonExistentFile = Paths.get(testingFolderPath.toString(), "Santa_Clause").toFile();
		try {
			BlueObjectInput.openDataInputStream(nonExistentFile);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_hasNext() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		}

		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertTrue(inStream.hasNext());
				assertTrue(inStream.hasNext());  // just to make sure it works multiple times
				assertEquals(value, inStream.next());
				assertNull(inStream.next());
				inStream.close();
			}
		}
	}

	@Test
	public void test_readLastBytes() throws Exception {
		TestValue firstValue = new TestValue("Jobodo Monobodo");
		TestValue secondValue = new TestValue("la la la");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(firstValue);
			outStream.write(secondValue);
			outStream.close();
		}

		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertNull(inStream.getLastBytes());

				assertTrue(inStream.hasNext());
				assertNull(inStream.getLastBytes());  // hasNext should not populate lastBytes

				assertEquals(firstValue, inStream.next());
				byte[] firstValueBytes = inStream.getLastBytes();
				TestValue restoredFirstValue = (TestValue) serializer.deserializeObjectFromByteArray(firstValueBytes);
				assertEquals(firstValue, restoredFirstValue);
				assertEquals(firstValue, restoredFirstValue);

				assertTrue(inStream.hasNext());
				assertEquals(firstValueBytes, inStream.getLastBytes());  // hasNext should not re-populate out lastBytes

				assertEquals(secondValue, inStream.next());
				byte[] secondValueBytes = inStream.getLastBytes();
				TestValue restoredSecondValue = (TestValue) serializer.deserializeObjectFromByteArray(secondValueBytes);
				assertEquals(secondValue, restoredSecondValue);

				assertFalse(inStream.hasNext());
				assertEquals(secondValueBytes, inStream.getLastBytes());  // hasNext should not clear out lastBytes

				inStream.close();
			}
		}
	}

	@Test
	public void test_peek() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		}

		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertNotNull(inStream.peek());
				assertNotNull(inStream.peek());  // just to make sure it works multiple times
				assertEquals(value, inStream.next());
				assertNull(inStream.peek());
				inStream.close();
			}
		}
	}

	@Test
	public void test_next() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		}

		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertEquals(value, inStream.next());
				assertNull(inStream.next());
				inStream.close();
			}
		}
	}

	@Test
	public void test_nextWithoutDeserializing() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		byte[] valueBytes = serializer.serializeObjectToByteArray(value);
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		}

		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertArrayEquals(valueBytes, inStream.nextWithoutDeserializing());
				assertNull(inStream.next());
				inStream.close();
			}
		}
	}

	@Test
	public void test_nextFromFile() throws Exception {
		File corruptedFile = createEmptyFile("test_nextFromFile");
		
		try(DataOutputStream outStream = new DataOutputStream(new FileOutputStream(corruptedFile))) {
			outStream.writeInt(20);
			byte[] junk = new byte[]{1, 2, 3};
			outStream.write(junk);
			outStream.close();
		}

		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(corruptedFile.toPath())) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertNull(inStream.next());
				inStream.close();
			}
		}
	}


	@Test
	public void test_nextFromFile_IOException() {
		AtomicBoolean readCalled = new AtomicBoolean(false);
		DataInputStream dataInputStream = createDataInputStreamThatThrowsExceptionOnRead(readCalled);
		BlueObjectInput<TestValue> inStream = BlueObjectInput.getTestInput(targetFilePath, serializer, dataInputStream);
		assertFalse(readCalled.get());
		assertNull(inStream.next());
		assertTrue(readCalled.get());
		inStream.close();
	}

	@Test
	public void test_close_exception() {
		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			Path path = readLock.getKey();
			AtomicBoolean streamClosed = new AtomicBoolean(false);
			DataInputStream inStream = createDataInputStreamThatThrowsExceptionOnClose(streamClosed);
			BlueObjectInput<TestValue> mockStream = BlueObjectInput.getTestInput(path, serializer, inStream);
			mockStream.close();  // BlueObjectInput should handle the exception
			assertTrue(streamClosed.get());  // make sure it actually closed the underlying stream
		}
	}
	

	@Test
	public void test_nextValidObjectFromFile_invalid() throws Exception {
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		Path garbagePath = TestUtils.getResourcePath("good-bad-good-stream.bin");
		BlueReadLock<Path> readLock = lockManager.acquireReadLock(garbagePath);
		BlueObjectInput<Call> inStream = new BlueObjectInput<>(readLock, serializer);
		int count = 0;
		while (inStream.hasNext()) {
			count += 1;
			inStream.next();
		}
		assertEquals(2, count);
		inStream.close();
	}

	private File createEmptyFile(String filename) throws IOException {
		File file = Paths.get(testingFolderPath.toString(), filename).toFile();
		file.getParentFile().mkdirs();
		file.createNewFile();
		return file;
	}

	private static DataInputStream createDataInputStreamThatThrowsExceptionOnRead(AtomicBoolean isRead){
		InputStream inputStream = new InputStream() {
			@Override
			public int read() throws IOException {
				isRead.set(true);
				throw new IOException();
			}
		};
		return new DataInputStream(inputStream);
	}

	private static DataInputStream createDataInputStreamThatThrowsExceptionOnClose(AtomicBoolean isClosed) {
		InputStream inputStream = new InputStream() {
			@Override
			public int read() throws IOException {
				return 0;
			}
			@Override
			public void close() throws IOException {
				isClosed.set(true);
				throw new IOException("fail!");
			}
		};
		return new DataInputStream(inputStream);
	}
}