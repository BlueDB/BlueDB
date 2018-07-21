package io.bluedb.disk.file;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.lock.BlueReadLock;
import io.bluedb.disk.lock.BlueWriteLock;
import io.bluedb.disk.lock.LockManager;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectInputTest extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;
	LockManager<Path> lockManager;
	Path testingFolderPath;
	Path targetFilePath;
	Path tempFilePath;

	@Override
	protected void setUp() throws Exception {
		testingFolderPath = Files.createTempDirectory(this.getClass().getSimpleName());
		targetFilePath = Paths.get(testingFolderPath.toString(), "BlueObjectOutputStreamTest.test_junk");
		tempFilePath = FileManager.createTempFilePath(targetFilePath);
		serializer = new ThreadLocalFstSerializer(new Class[]{});
		fileManager = new FileManager(serializer);
		lockManager = fileManager.getLockManager();
	}

	@Override
	protected void tearDown() throws Exception {
		targetFilePath.toFile().delete();
		tempFilePath.toFile().delete();
		recursiveDelete(testingFolderPath.toFile());
	}

	@Test
	public void test_close() {
		File emptyFile = createEmptyFile("your_cold_heart");
		
		try (BlueReadLock<Path> writeLock = lockManager.acquireReadLock(emptyFile.toPath())) {
			BlueObjectInput<TestValue> stream = fileManager.getBlueInputStream(writeLock);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(emptyFile.exists());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
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
	public void test_hasNext() {
		TestValue value = new TestValue("Jobodo Monobodo");
		BlueObjectInput<TestValue> inStream = null;
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			inStream = fileManager.getBlueInputStream(readLock);
			assertTrue(inStream.hasNext());
			assertTrue(inStream.hasNext());  // just to make sure it works multiple times
			assertEquals(value, inStream.next());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		assertNull(inStream.next());
	}

	@Test
	public void test_next() {
		TestValue value = new TestValue("Jobodo Monobodo");
		BlueObjectInput<TestValue> inStream = null;
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			inStream = fileManager.getBlueInputStream(readLock);
			assertEquals(value, inStream.next());
			inStream = fileManager.getBlueInputStream(readLock);
			assertEquals(value, inStream.next());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		assertNull(inStream.next());
	}

	@Test
	public void test_nextFromFile() {
		File corruptedFile = createEmptyFile("test_nextFromFile");
		
		try(DataOutputStream outStream = new DataOutputStream(new FileOutputStream(corruptedFile))) {
			outStream.writeInt(20);
			byte[] junk = new byte[]{1, 2, 3};
			outStream.write(junk);
			outStream.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
			fail();
		} catch (IOException e1) {
			e1.printStackTrace();
			fail();
		}

		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(corruptedFile.toPath())) {
			BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock);
			assertNull(inStream.next());
		} catch (BlueDbException e) {
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
	}

	@Test
	public void test_close_exception() {
		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			Path path = readLock.getKey();
			AtomicBoolean streamClosed = new AtomicBoolean(false);
			DataInputStream inStream = createDataInputStreamThatThrowsExceptionOnClose(streamClosed);
			BlueObjectInput<TestValue> mockStream = BlueObjectInput.getTestInput(path, serializer, inStream);
			mockStream.close();
			assertTrue(streamClosed.get());  // make sure it actually closed the underlying stream
		} catch (Exception e) {
			e.printStackTrace();
			fail();  // BlueObjectInput should have handled the exception
		}
	}
	

	private File createEmptyFile(String filename) {
		File file = Paths.get(testingFolderPath.toString(), filename).toFile();
		try {
			file.getParentFile().mkdirs();
			file.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
			fail();
		}
		return file;
	}

	private void recursiveDelete(File file) {
		if (!file.exists()) {
			return;
		} else if (file.isDirectory()) {
			for (File f: file.listFiles()) {
				recursiveDelete(f);
			}
			file.delete();
		} else {
			file.delete();
		}
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