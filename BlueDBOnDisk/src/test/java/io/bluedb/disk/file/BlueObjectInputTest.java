package io.bluedb.disk.file;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class BlueObjectInputTest extends TestCase {

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
		// TODO
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
}