package io.bluedb.disk.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.TestValue;
import io.bluedb.disk.lock.BlueReadLock;
import io.bluedb.disk.lock.BlueWriteLock;
import io.bluedb.disk.lock.LockManager;
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
			Blutils.recursiveDelete(file);
	}

	
	
	@Test
	public void test_loadObject() throws Exception {
		TestValue value = new TestValue("joe", 1);
		File nonExistantFile = new File("nonExistanceTestFile");
		File emptyFile = createFile("emptyTestFile");
		File corruptedFile = createCorruptedFile("corruptedTestFile");
		File fileWithValue = new File("testFileWithValue");
		filesToDelete.add(nonExistantFile);
		filesToDelete.add(emptyFile);
		filesToDelete.add(corruptedFile);
		filesToDelete.add(fileWithValue);

		fileManager.saveObject(fileWithValue.toPath(), value);

		Object nonExistantObject = fileManager.loadObject(nonExistantFile);
		Object emptyObject = fileManager.loadObject(emptyFile);
		Object validObject = fileManager.loadObject(fileWithValue);
		assertNull(nonExistantObject);
		assertNull(emptyObject);
		assertEquals(value, validObject);
	}

	@Test
	public void test_saveObject() throws Exception {
		TestValue value = new TestValue("joe", 1);
		File fileWithNull = createFile("testFileWithNull");
		File fileWithValue = new File("testFileWithValue");
		filesToDelete.add(fileWithNull);
		filesToDelete.add(fileWithValue);

		fileManager.saveObject(fileWithValue.toPath(), value);
		Object reloadedObject = fileManager.loadObject(fileWithValue.toPath());
		assertEquals(value, reloadedObject);

		fileManager.saveObject(fileWithNull.toPath(), null);
		Object reloadedNull = fileManager.loadObject(fileWithNull.toPath());
		assertNull(reloadedNull);
	}

	@Test
	public void test_getOutputStream() throws Exception {
		Path path = Paths.get("test_getOutputStream");
		filesToDelete.add(path.toFile());
		String string1 = "la la la la";
		String string2 = "1 2 3";
		try (BlueWriteLock<Path> writeLock = lockManager.acquireWriteLock(path)) {
			try (BlueObjectOutput<String> outStream = fileManager.getBlueOutputStream(writeLock)) {
				outStream.write(string1);
				outStream.write(string2);
			}
		}
		LockManager<Path> lockManager = fileManager.getLockManager();
		try (BlueReadLock<Path> readLock = lockManager.acquireReadLock(path)) {
			try (BlueObjectInput<String> inStream = fileManager.getBlueInputStream(readLock)) {
				assertEquals(string1, inStream.next());
				assertEquals(string2, inStream.next());
				assertNull(inStream.next());
			}
		}
	}

	@Test
	public void test_multithreaded() throws Exception {
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
	public void test_getReadLockIfFileExists() throws Exception {
		File testFolder = createTempFolder("test_getReadLockIfFileExists");
		filesToDelete.add(testFolder);
		Path nonExisting = Paths.get(testFolder.toString(), "non existing");
		Path existing = Paths.get(testFolder.toString(), "existing");
		existing.toFile().createNewFile();
		BlueReadLock<Path> nonExistingLock = fileManager.getReadLockIfFileExists(nonExisting);
		BlueReadLock<Path> existingLock = fileManager.getReadLockIfFileExists(existing);
		assertNull(nonExistingLock);
		assertNotNull(existingLock);
		existingLock.release();
		
		FileManager mockFileManager = new FileManager(null) {
			@Override
			public boolean exists(Path path) {
				throw new RuntimeException();
			}
		};
		try {
			BlueReadLock<Path> failedLockOnExistingFile = mockFileManager.getReadLockIfFileExists(existing);
			fail();
		} catch(BlueDbException expectedException) {
		}
		assertFalse(mockFileManager.getLockManager().isLocked(existing));
	}

	@Test
	public void test_getFolderContents_suffix() throws Exception {
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
	public void test_ensureFileExists() throws Exception {
		Path targetFilePath = Paths.get(testPath.toString(), "test_ensureFileExists");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureFileExists(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.deleteFile(lock);
		}
	}

	@Test
	public void test_ensureFileExists_already_existing() throws Exception {
		Path targetFilePath = Paths.get(testPath.toString(), "test_ensureFileExists");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureFileExists(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.ensureFileExists(targetFilePath);  // make sure we can do it after it already exists
			assertTrue(targetFilePath.toFile().exists());
			FileManager.deleteFile(lock);
		}
	}

	@Test
	public void test_createEmptyFile() throws Exception {
		Path targetFilePath = Paths.get(testPath.toString(), "test_ensureFileExists");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureDirectoryExists(targetFilePath.toFile());
			assertFalse(targetFilePath.toFile().exists());
			FileManager.createEmptyFile(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.createEmptyFile(targetFilePath);  // make sure a second call doesn't error out
			FileManager.deleteFile(lock);
		}

		Path fileInNonExistingFolder = Paths.get(testPath.toString(), "non_existing_folder", "test_ensureFileExists");
		try {
			assertFalse(fileInNonExistingFolder.toFile().exists());
			FileManager.createEmptyFile(fileInNonExistingFolder);
			fail();
		} catch (BlueDbException e) {
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
	public void test_moveFile() throws Exception {
		Path targetFilePath = Paths.get(testPath.toString(), "test_move");
		Path tempFilePath = FileManager.createTempFilePath(targetFilePath);
		filesToDelete.add(targetFilePath.toFile());
		filesToDelete.add(tempFilePath.toFile());

		FileManager.ensureDirectoryExists(tempFilePath.toFile());
		tempFilePath.toFile().createNewFile();
		assertTrue(tempFilePath.toFile().exists());
		assertFalse(targetFilePath.toFile().exists());
		fileManager.lockMoveFileUnlock(tempFilePath, targetFilePath);
		assertFalse(tempFilePath.toFile().exists());
		assertTrue(targetFilePath.toFile().exists());

		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		Path nonExistingFileTemp = FileManager.createTempFilePath(targetFilePath);
		try {
			fileManager.lockMoveFileUnlock(nonExistingFileTemp, nonExistingFile);
			fail();
		} catch ( BlueDbException e) {
		}
	
	}

	@Test
	public void test_deleteFile() throws Exception {
		Path targetFilePath = Paths.get(testPath.toString(), "test_deleteFile");
		filesToDelete.add(targetFilePath.toFile());
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(targetFilePath)) {
			FileManager.ensureFileExists(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileManager.deleteFile(lock);
			assertFalse(targetFilePath.toFile().exists());
		}
	}

	@Test
	public void test_readBytes_lock_invalid() throws Exception {
		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		try (BlueReadLock<Path> lock = lockManager.acquireReadLock(nonExistingFile)) {
			assertNull(fileManager.readBytes(lock));
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
	public void test_writeBytes_invalid() throws Exception {
		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		try (BlueWriteLock<Path> lock = lockManager.acquireWriteLock(nonExistingFile)) {
			byte[] bytes = new byte[] {1, 2, 3};
			fileManager.writeBytes(lock, bytes);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_moveWithoutLock() throws Exception {
		File srcFolder = createTempFolder("test_moveWithoutLock_src");
		File dstFolder = createTempFolder("test_moveWithoutLock_dst");
		Path srcFilePath = Paths.get(srcFolder.toPath().toString(), "test_file");
		Path dstFilePath = Paths.get(dstFolder.toPath().toString(), "test_file");
		File srcFile = srcFilePath.toFile();
		File dstFile = dstFilePath.toFile();

		assertFalse(srcFile.exists());
		srcFilePath.toFile().createNewFile();
		assertTrue(srcFile.exists());
		assertFalse(dstFile.exists());
		FileManager.moveWithoutLock(srcFilePath, dstFilePath);
		assertFalse(srcFile.exists());
		assertTrue(dstFile.exists());
	}

	@Test
	public void test_moveWithoutLock_exception() throws Exception {
		File srcFolder = createTempFolder("test_moveWithoutLock_exception_src");
		File dstFolder = createTempFolder("test_moveWithoutLock_exception_dst");
		Path srcFilePath = Paths.get(srcFolder.toPath().toString(), "test_file");
		Path dstFilePath = Paths.get(dstFolder.toPath().toString(), "test_file");
		File srcFile = srcFilePath.toFile();
		File dstFile = dstFilePath.toFile();

		try {
			assertFalse(srcFile.exists());
			assertFalse(dstFile.exists());
			FileManager.moveWithoutLock(srcFilePath, dstFilePath);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_copyFileWithoutLock() throws Exception {
		File srcFolder = createTempFolder("test_copyFileWithoutLock_src");
		File dstFolder = createTempFolder("test_copyFileWithoutLock_dst");
		Path srcFilePath = Paths.get(srcFolder.toPath().toString(), "test_file");
		Path dstFilePath = Paths.get(dstFolder.toPath().toString(), "test_file");
		File srcFile = srcFilePath.toFile();
		File dstFile = dstFilePath.toFile();

		assertFalse(srcFile.exists());
		srcFilePath.toFile().createNewFile();
		assertTrue(srcFile.exists());
		assertFalse(dstFile.exists());
		FileManager.copyFileWithoutLock(srcFilePath, dstFilePath);
		assertTrue(srcFile.exists());
		assertTrue(dstFile.exists());
	}

	@Test
	public void test_copyFileWithoutLock_exception() throws Exception {
		File srcFolder = createTempFolder("test_copyFileWithoutLock_exception_src");
		File dstFolder = createTempFolder("test_copyFileWithoutLock_exception_dst");
		Path srcFilePath = Paths.get(srcFolder.toPath().toString(), "test_file");
		Path dstFilePath = Paths.get(dstFolder.toPath().toString(), "test_file");
		File srcFile = srcFilePath.toFile();
		File dstFile = dstFilePath.toFile();

		try {
			assertFalse(srcFile.exists());
			assertFalse(dstFile.exists());
			FileManager.copyFileWithoutLock(srcFilePath, dstFilePath);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_copyDirectoryWithoutLock() throws Exception {
		File testFolder = createTempFolder("test_copyDirectoryWithoutLock");
		Path srcPath = Paths.get(testFolder.toPath().toString(), "src");
		Path srcFilePath = Paths.get(srcPath.toString(), "file");
		Path srcSubfolderPath = Paths.get(srcPath.toString(), "subfolder");
		Path srcSubfolderFilePath = Paths.get(srcSubfolderPath.toString(), "subfolder_file");

		Path dstPath = Paths.get(testFolder.toPath().toString(), "dst");
		Path dstFilePath = Paths.get(dstPath.toString(), "file");
		Path dstSubfolderPath = Paths.get(dstPath.toString(), "subfolder");
		Path dstSubfolderFilePath = Paths.get(dstSubfolderPath.toString(), "subfolder_file");

		srcPath.toFile().mkdirs();
		srcFilePath.toFile().createNewFile();
		srcSubfolderPath.toFile().mkdirs();
		srcSubfolderFilePath.toFile().createNewFile();
		assertTrue(srcPath.toFile().exists());
		assertTrue(srcFilePath.toFile().exists());
		assertTrue(srcSubfolderPath.toFile().exists());
		assertTrue(srcSubfolderFilePath.toFile().exists());

		assertFalse(dstPath.toFile().exists());
		assertFalse(dstFilePath.toFile().exists());
		assertFalse(dstSubfolderPath.toFile().exists());
		assertFalse(dstSubfolderFilePath.toFile().exists());
		FileManager.copyDirectoryWithoutLock(srcPath, dstPath);
		assertTrue(dstPath.toFile().exists());
		assertTrue(dstFilePath.toFile().exists());
		assertTrue(dstSubfolderPath.toFile().exists());
		assertTrue(dstSubfolderFilePath.toFile().exists());
	}

	@Test
	public void test_copyDirectoryWithoutLock_not_a_directory() throws Exception {
		Path tempFile = null;
		tempFile = Files.createTempFile(this.getClass().getSimpleName() + "_test_copyDirectoryWithoutLock_not_a_directory", ".tmp");
		tempFile.toFile().deleteOnExit();

		Path destination = Paths.get("never_going_to_happen");
		try {
			FileManager.copyDirectoryWithoutLock(tempFile, destination);
			fail();
		} catch (BlueDbException e) {
		}
	}

	@Test
	public void test_isTempFile() throws Exception {
		File file = Paths.get(".", "whatever").toFile();
		File tempFile = FileManager.createTempFilePath(file.toPath()).toFile();
		assertFalse(FileManager.isTempFile(file));
		assertTrue(FileManager.isTempFile(tempFile));
		assertFalse(FileManager.isTempFile(null));
	}


	private File createFile(String fileName) throws Exception {
		File file = new File(fileName);
		file.createNewFile();
		return file;
	}

	private File createCorruptedFile(String fileName) throws FileNotFoundException, IOException {
		File file = new File(fileName);
		byte[] junk = new byte[] { 3, 1, 2 };
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(junk);
			fos.close();
		}
		return file;
	}

	private File createFile(File parentFolder, String fileName) throws IOException {
		File file = Paths.get(parentFolder.toPath().toString(), fileName).toFile();
		file.createNewFile();
		return file;
	}

	private File createFileAndWriteTestValue(String pathString, TestValue value) throws BlueDbException {
		File file = new File(pathString);
		fileManager.saveObject(file.toPath(), value);
		return file;
	}

	private void loadAddCupcakeAndSave(Path path) throws BlueDbException {
		TestValue value = (TestValue) fileManager.loadObject(path);
		value.addCupcake();
		fileManager.saveObject(path, value);
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

	private File createTempFolder(String tempFolderName) throws IOException {
		Path tempFolderPath = Files.createTempDirectory(tempFolderName);
		File tempFolder = tempFolderPath.toFile();
		tempFolder.deleteOnExit();
		filesToDelete.add(tempFolder);
		return tempFolder;
	}
}
