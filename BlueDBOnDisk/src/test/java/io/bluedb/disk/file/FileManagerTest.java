package io.bluedb.disk.file;

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

public class FileManagerTest extends TestCase {

	BlueSerializer serializer;
	FileManager fileManager;
	LockManager<Path> lockManager;
	private List<File> filesToDelete;
	private Path testPath;

	@Override
	protected void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[] {});
		fileManager = new FileManager(serializer);
		lockManager = fileManager.getLockManager();
		filesToDelete = new ArrayList<>();
		testPath = Paths.get(".", "test_" + this.getClass().getSimpleName());
		filesToDelete.add(testPath.toFile());
	}

	@Override
	protected void tearDown() throws Exception {
		for (File file : filesToDelete)
			recursiveDelete(file);
	}

	
	
	@Test
	public void test_loadObject() {
		TestValue value = new TestValue("joe", 1);
		File nonExistantFile = new File("nonExistanceTestFile");
		File emptyFile = createFile("emptyTestFile");
		File corruptedFile = createCorruptedFile("corruptedTestFile");
		File fileWithValue = new File("testFileWithValue");
		filesToDelete.add(nonExistantFile);
		filesToDelete.add(emptyFile);
		filesToDelete.add(corruptedFile);
		filesToDelete.add(fileWithValue);
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
		filesToDelete.add(fileWithNull);
		filesToDelete.add(fileWithValue);

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
	public void test_getOutputStream() {
		Path path = Paths.get("test_getOutputStream");
		filesToDelete.add(path.toFile());
		String string1 = "la la la la";
		String string2 = "1 2 3";
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(path)) {
			try (BlueObjectOutput<String> outStream = fileManager.getBlueOutputStream(writeLock)) {
				outStream.write(string1);
				outStream.write(string2);
			} catch (BlueDbException e) {
				e.printStackTrace();
				fail();
			}
		}
		LockManager<Path> lockManager = fileManager.getLockManager();
		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(path)) {
			try (BlueObjectInput<String> inStream = fileManager.getBlueInputStream(readLock)) {
				assertEquals(string1, inStream.next());
				assertEquals(string2, inStream.next());
				assertNull(inStream.next());
			} catch (BlueDbException e) {
				e.printStackTrace();
				fail();
			}
		}
	}

	@Test
	public void test_multithreaded() {
		TestValue value = new TestValue("joe", 0);
		File file = createFileAndWriteTestValue("testMultithreaded", value);
		filesToDelete.add(file);
		Path path = file.toPath();

		int attemptsToOpen = 500;
		CountDownLatch doneSignal = new CountDownLatch(attemptsToOpen);
		AtomicLong successfulOpens = new AtomicLong(0);

		List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < attemptsToOpen; i++) {
			Runnable reader = new Runnable() {
				@Override
				public void run() {
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
		while (doneSignal.getCount() > 0) {
			if (!threads.isEmpty()) {
				Thread thread = threads.remove(threads.size() - 1);
				thread.start();
			}
			loadAddCupcakeAndSave(path);
		}

		assertEquals(attemptsToOpen, successfulOpens.get());
	}

	@Test
	public void test_getFolderContents_suffix() {
		String suffix = ".foo";
		File nonExistant = new File("forever_or_never_whatever");
		filesToDelete.add(nonExistant);
		List<File> emptyFileList = new ArrayList<>();
		assertEquals(emptyFileList, FileManager.getFolderContents(nonExistant.toPath(), suffix));

		File emptyFolder = new File("bah_bah_black_sheep");
		filesToDelete.add(emptyFolder);
		emptyFolder.mkdirs();
		assertEquals(emptyFileList, FileManager.getFolderContents(emptyFolder.toPath(), suffix));

		File nonEmptyFolder = new File("owa_tana_siam");
		filesToDelete.add(nonEmptyFolder);
		nonEmptyFolder.mkdirs();
		File fileWithSuffix = createFile(nonEmptyFolder, "legit" + suffix);
		File fileWithSuffix2 = createFile(nonEmptyFolder, "legit.stuff" + suffix);
		File fileWithSuffixInMiddle = createFile(nonEmptyFolder, "not" + suffix + ".this");
		createFile(nonEmptyFolder, "junk");
		List<File> filesWithSuffix = FileManager.getFolderContents(nonEmptyFolder.toPath(), suffix);
		assertEquals(2, filesWithSuffix.size());
		assertTrue(filesWithSuffix.contains(fileWithSuffix));
		assertTrue(filesWithSuffix.contains(fileWithSuffix2));
	}

	@Test
	public void test_ensureFileExists() {
		Path targetFilePath = Paths.get(testPath.toString(), "test_ensureFileExists");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureFileExists(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.deleteFile(lock);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_createEmptyFile() {
		Path targetFilePath = Paths.get(testPath.toString(), "test_ensureFileExists");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureDirectoryExists(targetFilePath.toFile());
			assertFalse(targetFilePath.toFile().exists());
			FileManager.createEmptyFile(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.createEmptyFile(targetFilePath);  // make sure a second call doesn't error out
			FileManager.deleteFile(lock);
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}

		Path fileInNonExistingFolder = Paths.get(testPath.toString(), "non_existing_folder", "test_ensureFileExists");
		try {
			assertFalse(fileInNonExistingFolder.toFile().exists());
			FileManager.createEmptyFile(fileInNonExistingFolder);
			fail();
		} catch (BlueDbException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test_ensureDirectoryExists() {
		Path targetFilePath = Paths.get(testPath.toString(), "test_ensureDirectoryExists",
				"test_ensureDirectoryExists");
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			assertFalse(targetFilePath.getParent().toFile().exists());
			FileManager.ensureDirectoryExists(targetFilePath.toFile());
			assertTrue(targetFilePath.getParent().toFile().exists());
			FileManager.ensureDirectoryExists(targetFilePath.toFile()); // make sure a second one doesn't error out
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

	@Test
	public void test_moveFile() {
		Path targetFilePath = Paths.get(testPath.toString(), "test_move");
		Path tempFilePath = FileManager.createTempFilePath(targetFilePath);
		filesToDelete.add(targetFilePath.toFile());
		filesToDelete.add(tempFilePath.toFile());
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

		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		Path nonExistingFileTemp = FileManager.createTempFilePath(targetFilePath);
		try {
			fileManager.lockMoveFileUnlock(nonExistingFileTemp, nonExistingFile);
			fail();
		} catch ( BlueDbException e) {
			e.printStackTrace();
		}
	
	}

	@Test
	public void test_deleteFile() {
		Path targetFilePath = Paths.get(testPath.toString(), "test_deleteFile");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureFileExists(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.deleteFile(lock);
			assertFalse(targetFilePath.toFile().exists());
		} catch (BlueDbException e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	public void test_readBytes_lock_invalid() {
		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		try (BlueReadLock<Path> lock = lockManager.acquireReadLock(nonExistingFile)) {
			assertNull(fileManager.readBytes(lock));
		} catch ( BlueDbException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void test_readBytes_path_invalid() {
		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		try {
			fileManager.readBytes(nonExistingFile);
			fail();
		} catch ( BlueDbException e) {
		}
	}
	
	@Test
	public void test_writeBytes_invalid() {
		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(nonExistingFile)) {
			byte[] bytes = new byte[] {1, 2, 3};
			fileManager.writeBytes(lock, bytes);
			fail();
		} catch ( BlueDbException e) {
			e.printStackTrace();
		}
	}
	
	
	private void recursiveDelete(File file) {
		if (!file.exists()) {
			return;
		} else if (file.isDirectory()) {
			for (File f : file.listFiles()) {
				recursiveDelete(f);
			}
			file.delete();
		} else {
			file.delete();
		}
	}

	private File createFile(String fileName) {
		File file = new File(fileName);
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
		byte[] junk = new byte[] { 3, 1, 2 };
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
