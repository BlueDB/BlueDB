package io.bluedb.disk.file;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.serialization.BlueSerializer;
import io.bluedb.disk.serialization.ThreadLocalFstSerializer;
import junit.framework.TestCase;

public class FileManagerTest  extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;
	List<File> filesAndFoldersToCleanUp;

	@Override
	protected void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[]{});
		fileManager = new FileManager(serializer);
		filesAndFoldersToCleanUp = new ArrayList<>();
	}

	@Override
	protected void tearDown() throws Exception {
		for (File file: filesAndFoldersToCleanUp)
			recursiveDelete(file);
	}

	@Test
	public void test_getOutputStream() {
		Path path = Paths.get("test_getOutputStream");
		filesAndFoldersToCleanUp.add(path.toFile());
		String string1 = "la la la la";
		String string2 = "1 2 3";
		try (BlueObjectOutputStream<String> outStream = fileManager.getBlueOutputStream(path)) {
			outStream.write(string1);
			outStream.write(string2);
			outStream.commit();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		} catch (IOException e1) {
			e1.printStackTrace();
			fail();
		}
		try (BlueObjectInputStream<String> inStream = fileManager.getBlueInputStream(path)) {
			assertEquals(string1, inStream.next());
			assertEquals(string2, inStream.next());
			assertEquals("should never get here", inStream.next());
			fail();
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		} catch (EOFException e1) {
		} catch (IOException e2) {
			e2.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_createTempFilePath() {
		Path withParent = Paths.get("grandparent", "parent", "target");
		Path tempWithParent = FileManager.createTempFilePath(withParent);
		Path expectedTempWithParent = Paths.get("grandparent", "parent", "_tmp_target");
		assertEquals(expectedTempWithParent, tempWithParent);

		Path withoutParent = Paths.get("target");
		Path tempWithoutParent = FileManager.createTempFilePath(withoutParent);
		Path expectedTempWithoutParent = Paths.get("_tmp_target");
		assertEquals(expectedTempWithoutParent, tempWithoutParent);
	}

	// TODO test multiple files and/or files with suffix not at the end
	@Test
	public void test_listFiles() {
		String suffix = ".foo";
		File nonExistant = new File("forever_or_never_whatever");
		List<File> emptyFileList = new ArrayList<>();
		assertEquals(emptyFileList, fileManager.listFiles(nonExistant.toPath(), suffix));

		File emptyFolder = new File("bah_bah_black_sheep");
		emptyFolder.mkdirs();
		assertEquals(emptyFileList, fileManager.listFiles(emptyFolder.toPath(), suffix));

		File nonEmptyFolder = new File("owa_tana_siam");
		nonEmptyFolder.mkdirs();
		File fileWithSuffix = createFile(nonEmptyFolder, "legit" + suffix);
		createFile(nonEmptyFolder, "junk");
		List<File> filesWithSuffix = fileManager.listFiles(nonEmptyFolder.toPath(), suffix);
		assertEquals(1, filesWithSuffix.size());
		assertTrue(filesWithSuffix.contains(fileWithSuffix));
	}

	@Test
	public void test_loadObject() {
		TestValue value = new TestValue("joe", 1);
		File nonExistantFile = new File("nonExistanceTestFile");
		File emptyFile = createFile("emptyTestFile");
		File corruptedFile = createCorruptedFile("corruptedTestFile");
		File fileWithValue = new File("testFileWithValue");
		try {
			fileManager.saveObject(fileWithValue.toPath(), value);

			Object nonExistantObject = fileManager.loadObject(nonExistantFile.toPath());
			Object emptyObject = fileManager.loadObject(emptyFile.toPath());
			Object validObject = fileManager.loadObject(fileWithValue.toPath());
			assertNull(nonExistantObject);
			assertNull(emptyObject);
			assertEquals(value, validObject);
			
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_saveObject() {
		TestValue value = new TestValue("joe", 1);
		File fileWithNull = createFile("testFileWithNull");
		File fileWithValue = new File("testFileWithValue");
		filesAndFoldersToCleanUp.add(fileWithValue);
		filesAndFoldersToCleanUp.add(fileWithNull);

		try {
			fileManager.saveObject(fileWithValue.toPath(), value);
			Object reloadedObject = fileManager.loadObject(fileWithValue.toPath());
			assertEquals(value, reloadedObject);
			
			fileManager.saveObject(fileWithNull.toPath(), null);
			Object reloadedNull = fileManager.loadObject(fileWithNull.toPath());
			assertNull(reloadedNull);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_multithreaded() {
		TestValue value = new TestValue("joe", 0);
		File file = createFileAndWriteTestValue("testMultithreaded", value);
		Path path = file.toPath();

		int attemptsToOpen = 500;
		CountDownLatch doneSignal = new CountDownLatch(attemptsToOpen);
		AtomicLong successfulOpens = new AtomicLong(0);

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < attemptsToOpen; i++) {
			Runnable reader = new Runnable() {
				@Override public void run() {
					TestValue loadedValue = loadTestValueFromPathOrNull(path);
					if (loadedValue != null) {
						successfulOpens.incrementAndGet();
					}
					doneSignal.countDown();
				}
			};
			Thread readerThread = new Thread(reader);
			threads.add(readerThread);
		}
		
		// interleave writing and reading
		while(doneSignal.getCount() > 0) {
			if (!threads.isEmpty()) {
				Thread thread = threads.remove(threads.size()-1);
				thread.start();
			}
			loadAddCupcakeAndSave(path);
		}

		assertEquals(attemptsToOpen, successfulOpens.get());
	}

	@Test
	public void test_moveFile() {
		Path targetFilePath = Paths.get(this.getClass().getSimpleName() + ".test_junk");
		Path tempFilePath = FileManager.createTempFilePath(targetFilePath);
		filesAndFoldersToCleanUp.add(targetFilePath.toFile());
		filesAndFoldersToCleanUp.add(tempFilePath.toFile());
		try {
			FileManager.ensureDirectoryExists(tempFilePath.toFile());
			tempFilePath.toFile().createNewFile();
			assertTrue(tempFilePath.toFile().exists());
			assertFalse(targetFilePath.toFile().exists());
			fileManager.lockMoveFileUnlock(tempFilePath, targetFilePath);
			assertFalse(tempFilePath.toFile().exists());
			assertTrue(targetFilePath.toFile().exists());
		} catch (IOException | BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_lockMoveFileUnlock() {
		// TODO
	}

	@Test
	public void test_loadList() {
		// TODO
	}

	@Test
	public void test_saveList() {
		// TODO
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

	private File createFile(String fileName) {
		File file = new File(fileName);
		filesAndFoldersToCleanUp.add(file);
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private File createCorruptedFile(String fileName) {
		File file = new File(fileName);
		filesAndFoldersToCleanUp.add(file);
		byte[] junk = new byte[] {3, 1, 2};
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(junk);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private File createFile(File parentFolder, String fileName) {
		File file = Paths.get(parentFolder.toPath().toString(), fileName).toFile();
		filesAndFoldersToCleanUp.add(file);
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private File createFileAndWriteTestValue(String pathString, TestValue value) {
		File file = new File(pathString);
		filesAndFoldersToCleanUp.add(file);
		try {
			fileManager.saveObject(file.toPath(), value);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
		return file;
	}

	private void loadAddCupcakeAndSave(Path path) {
		try {
			TestValue value = (TestValue) fileManager.loadObject(path);
			value.addCupcake();
			fileManager.saveObject(path, value);
		} catch (BlueDbException e) {
			System.out.println("attempt to save failed");
		}
	}

	private TestValue loadTestValueFromPathOrNull(Path path) {
		try {
			TestValue value = (TestValue) fileManager.loadObject(path);
			return value;
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
		return null;
	}
}

