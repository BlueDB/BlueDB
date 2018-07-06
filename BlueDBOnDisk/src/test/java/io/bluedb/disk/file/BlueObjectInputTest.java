package io.bluedb.disk.file;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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

public class BlueObjectInputTest extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;
	LockManager<Path> lockManager;
	Path testingFolderPath = Paths.get(".", "BlueObjectInputTest");
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
		System.out.println(testingFolderPath.toFile().listFiles());
		Files.walk(testingFolderPath)
			.sorted(Comparator.reverseOrder())
			.map(Path::toFile)
			.forEach(File::delete);
		testingFolderPath.toFile().delete();
		recursiveDelete(testingFolderPath.toFile());
		System.out.println(testingFolderPath.toFile().listFiles());
	}

	@Test
	public void test_close() {
		File emptyFile = Paths.get(testingFolderPath.toString(), "your_cold_heart").toFile();
		try {
			emptyFile.createNewFile();
		} catch (IOException e1) {
			e1.printStackTrace();
			fail();
		}
		
		try (BlueReadLock<Path> writeLock = lockManager.acquireReadLock(emptyFile.toPath())) {
			BlueObjectInput<TestValue> stream = fileManager.getBlueInputStream(writeLock);
			stream.close();
			stream.close();  // make sure it doesn't throw an exception if you close it twice
			assertTrue(targetFilePath.toFile().exists());
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
		File corruptedFile = Paths.get(testingFolderPath.toString(), "test_nextFromFile").toFile();
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