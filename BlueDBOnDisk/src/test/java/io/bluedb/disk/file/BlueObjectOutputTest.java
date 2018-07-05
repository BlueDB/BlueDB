package io.bluedb.disk.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectOutputTest extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;
	LockManager<Path> lockManager;
	Path testingFolder = Paths.get(".testing");
	Path targetFilePath = Paths.get(testingFolder.toString(), "BlueObjectOutputStreamTest.test_junk");
	Path tempFilePath = FileManager.createTempFilePath(targetFilePath);

	@Override
	protected void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[]{});
		fileManager = new FileManager(serializer);
		lockManager = fileManager.getLockManager();
	}

	@Override
	protected void tearDown() throws Exception {
		targetFilePath.toFile().delete();
		tempFilePath.toFile().delete();
		testingFolder.toFile().delete();
	}

	@Test
	public void test_close() {
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			assertFalse(targetFilePath.toFile().exists());
			BlueObjectOutput<TestValue> stream = fileManager.getBlueOutputStream(writeLock);
			stream.write(null);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(targetFilePath.toFile().exists());
		} catch (BlueDbException | IOException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_write() {
		TestValue value = new TestValue("Jobodo Monobodo");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}

		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock);
			assertEquals(value, inStream.next());
		} catch (BlueDbException | IOException e) {
			e.printStackTrace();
			fail();
		}
		// TODO test exception on write
	}
}