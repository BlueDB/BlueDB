package io.bluedb.disk.file;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
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
	Path testingFolderPath = Paths.get(".", "BlueObjectOutputTest");
	Path targetFilePath = Paths.get(testingFolderPath.toString(), "BlueObjectOutputStreamTest.test_junk");
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
		Files.walk(testingFolderPath)
			.sorted(Comparator.reverseOrder())
			.map(Path::toFile)
			.forEach(File::delete);
		testingFolderPath.toFile().delete();
		recursiveDelete(testingFolderPath.toFile());
	}

	@Test
	public void test_close() {
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> stream = fileManager.getBlueOutputStream(writeLock);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(targetFilePath.toFile().exists());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

//	@Test
//	public void test_openDataInputStream() {
//		File unavailableFile = Paths.get(testingFolder.toString(), "your_heart").toFile();
//		
//		try (FileChannel channel = new RandomAccessFile(unavailableFile, "rw").getChannel() ) {
//			try (FileLock lock = channel.lock()) {
//				try {
//					BlueObjectOutput.openDataOutputStream(unavailableFile);
//					fail();
//				} catch (BlueDbException e) {}
//			} catch (IOException e1) {
//				e1.printStackTrace();
//				fail();
//			}
//		} catch (IOException e2) {
//			e2.printStackTrace();
//			fail();
//		}
//	}

	@Test
	public void test_write() {
		TestValue value = new TestValue("Jobodo Monobodo");
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock);
			outStream.write(value);
			outStream.close();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}

		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(targetFilePath)) {
			BlueObjectInput<TestValue> inStream = fileManager.getBlueInputStream(readLock);
			assertEquals(value, inStream.next());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}

		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(targetFilePath)) {
			try (BlueObjectOutput<TestValue> outStream = fileManager.getBlueOutputStream(writeLock)) {
				outStream.write(null);
				fail();
			}
		} catch (BlueDbException e) {
		}
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
}