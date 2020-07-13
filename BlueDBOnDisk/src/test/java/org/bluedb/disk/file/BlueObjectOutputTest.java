package org.bluedb.disk.file;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.Mockito;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectOutputTest extends TestCase {

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
	protected void tearDown() throws Exception {
		targetFilePath.toFile().delete();
		tempFilePath.toFile().delete();
		Blutils.recursiveDelete(testingFolderPath.toFile());
	}

	
	
	@Test
	public void test_close_exception() throws Exception {
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			Path path = writeLock.getKey();
			AtomicBoolean dataOutputClosed = new AtomicBoolean(false);
			DataOutputStream outStream = createDataOutputStreamThatThrowsExceptionOnClose(path.toFile(), dataOutputClosed);
			BlueObjectOutput<TestValue> mockStream = BlueObjectOutput.getTestOutput(path, serializer, outStream);
			mockStream.close(); // BlueObjectOutput should handle the exception
			assertTrue(dataOutputClosed.get());  // make sure it actually closed the underlying stream
		}
	}
	
	@Test
	public void test_close() throws Exception {
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> stream = fileManager.getBlueOutputStream(writeLock);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(targetFilePath.toFile().exists());
		}
	}

	@Test
	public void test_write() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		}

		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertEquals(value, inStream.next());
				inStream.close();
			}
		}

		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			try (BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock)) {
				outStream.write(null);
				fail();
			}
		} catch (BlueDbException e) {
		}

		try {
			BlueObjectOutput<TestValue> invalidOut = BlueObjectOutput.getTestOutput(null, null, null);
			invalidOut.write(value);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_writeBytes() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		byte[] valueBytes = serializer.serializeObjectToByteArray(value);
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.writeBytes(valueBytes);
			outStream.close();
		}

		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			try (BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock)) {
				assertEquals(value, inStream.next());
				inStream.close();
			}
		}

		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			try (BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock)) {
				outStream.writeBytes(null);
				fail();
			}
		} catch (BlueDbException e) {
		}

		try {
			BlueObjectOutput<TestValue> invalidOut = BlueObjectOutput.getTestOutput(null, null, null);
			invalidOut.writeBytes(valueBytes);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_copyObjects() throws Exception {
		TestValue value = new TestValue("Jobodo Monobodo");
		Path srcPath = targetFilePath;
		Path dstPath = tempFilePath;
		
		// prepare the source file
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(srcPath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		}

		// copy
		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(srcPath)) {
			try (BlueObjectInput<TestValue> input = fileManager.getBlueInputStream(readLock)) {
				try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(dstPath)) {
					try (BlueObjectOutput<TestValue> output = fileManager.getBlueOutputStream(writeLock)) {
						output.writeAll(input);
						output.close();
					}
				}
				input.close();
			}
		}

		// confirm that it worked
		try(BlueReadLock<Path> readLock = lockManager.acquireReadLock(dstPath)) {
			try (BlueObjectInput<TestValue> input = fileManager.getBlueInputStream(readLock)) {
				assertEquals(value, input.next());
				input.close();
			}
		}
	}

	@Test
	public void test_constructor_exception() throws Exception {
		@SuppressWarnings("unchecked")
		BlueWriteLock<Path> lock = Mockito.mock(BlueWriteLock.class);
		Mockito.verify(lock, Mockito.times(0)).close();
		try {
			@SuppressWarnings({ "unused", "resource" })
			BlueObjectOutput<TestValue> stream = new BlueObjectOutput<>(lock, null);
		} catch (BlueDbException e) {}
		Mockito.verify(lock, Mockito.times(1)).close();
	}
	
	
	private static DataOutputStream createDataOutputStreamThatThrowsExceptionOnClose(File file, AtomicBoolean dataOutputClosed) throws BlueDbException {
		try {
			return new DataOutputStream(new FileOutputStream(file) {
				@Override
				public void close() throws IOException {
					super.close();
					dataOutputClosed.set(true);
					throw new IOException("fail!");
				}
			});
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new BlueDbException("cannot open write to file " + file.toPath(), e);
		}
	}
}