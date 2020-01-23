package org.bluedb.disk.file;

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.lock.BlueWriteLock;
import org.bluedb.disk.lock.LockManager;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;

import junit.framework.TestCase;

public class ReadWriteFileManagerTest extends TestCase {

	BlueSerializer serializer;
	ReadWriteFileManager fileManager;
	LockManager<Path> lockManager;
	private List<File> filesToDelete;
	private Path testPath;

	@Override
	protected void setUp() throws Exception {
		serializer = new ThreadLocalFstSerializer(new Class[] {});
		fileManager = new ReadWriteFileManager(serializer);
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
	public void test_saveAndLoadVersionedObjects() throws Exception {
		File tempDir = createTempFolder("test_saveVersionedObject");
		Path tempDirPath = tempDir.toPath();
		String filename = "test_filename";
		
		TestValue value1 = new TestValue("Joe Dirt", 5);
		Path value1Path = tempDirPath.resolve(filename);
		
		fileManager.saveObject(value1Path, value1);
		
		assertEquals(value1, fileManager.loadVersionedObject(tempDirPath, filename)); //It should load the legacy non-versioned object if its the only one
		FileTime value1LastModified = Files.getLastModifiedTime(value1Path);

		
		TestValue value2 = new TestValue("Michael Scott", 3);
		fileManager.saveVersionedObject(tempDirPath, filename, value2);
		
		Path value2Path = ReadWriteFileManager.getNewestVersionPath(tempDirPath, filename);
		FileTime value2LastModified = Files.getLastModifiedTime(value2Path);
		
		assertEquals(value1, fileManager.loadObject(value1Path));
		assertEquals(value1LastModified, Files.getLastModifiedTime(value1Path));
		assertEquals(value2, fileManager.loadVersionedObject(tempDirPath, filename));
		assertNotEquals(value2LastModified, value1LastModified);
		
		Thread.sleep(1);
		
		TestValue value3 = new TestValue("Leroy Jenkins", 0);
		fileManager.saveVersionedObject(tempDirPath, filename, value3);
		
		Path value3Path = ReadWriteFileManager.getNewestVersionPath(tempDirPath, filename);
		FileTime value3LastModified = Files.getLastModifiedTime(value3Path);
		
		assertEquals(value1, fileManager.loadObject(value1Path));
		assertEquals(value1LastModified, Files.getLastModifiedTime(value1Path));
		assertEquals(value2, fileManager.loadObject(value2Path));
		assertEquals(value2LastModified, Files.getLastModifiedTime(value2Path));
		assertEquals(value3, fileManager.loadVersionedObject(tempDirPath, filename));
		assertNotEquals(value3LastModified, value2LastModified);
	}
	
	@Test
	public void test_getNewestVersionPath() throws IOException {
		File tempDir = createTempFolder("test_getNewestVersionPath");
		Path tempDirPath = tempDir.toPath();
		String filename = "test_filename";
		
		Random r = new Random();
		SimpleDateFormat sdf = new SimpleDateFormat(ReadWriteFileManager.TIMESTAMP_VERSION_FORMAT);
		
		assertNull(ReadWriteFileManager.getNewestVersionPath(tempDirPath, filename));
		
		List<Path> testPaths = new LinkedList<Path>();
		
		Path legacyPath = tempDirPath.resolve(filename);
		Files.createFile(legacyPath);
		testPaths.add(legacyPath);
		
		assertEquals(legacyPath, ReadWriteFileManager.getNewestVersionPath(tempDirPath, filename));
		
		long maxTime = -1;
		Path maxPath = null;
		for(int i = 0; i < 1000; i++) {
			long time = Math.abs(r.nextLong()) % 4134005999000L;
			Path path = tempDirPath.resolve(filename + "_" + sdf.format(time));
			if(time > maxTime) {
				maxTime = time;
				maxPath = path;
			}
			Files.createFile(path);
			testPaths.add(path);
		}
		
		assertEquals(maxPath, ReadWriteFileManager.getNewestVersionPath(tempDirPath, filename));
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
		
		ReadWriteFileManager mockFileManager = new ReadWriteFileManager(null) {
			@Override
			public boolean exists(Path path) {
				throw new RuntimeException();
			}
		};
		try {
			mockFileManager.getReadLockIfFileExists(existing);
			fail();
		} catch(BlueDbException expectedException) {
		}
		assertFalse(mockFileManager.getLockManager().isLocked(existing));
	}

	@Test
	public void test_moveFile() throws Exception {
		Path targetFilePath = Paths.get(testPath.toString(), "test_move");
		Path tempFilePath = FileUtils.createTempFilePath(targetFilePath);
		filesToDelete.add(targetFilePath.toFile());
		filesToDelete.add(tempFilePath.toFile());

		FileUtils.ensureDirectoryExists(tempFilePath.toFile());
		tempFilePath.toFile().createNewFile();
		assertTrue(tempFilePath.toFile().exists());
		assertFalse(targetFilePath.toFile().exists());
		fileManager.lockMoveFileUnlock(tempFilePath, targetFilePath);
		assertFalse(tempFilePath.toFile().exists());
		assertTrue(targetFilePath.toFile().exists());

		Path nonExistingFile = Paths.get(testPath.toString(), "test_move_non_existing");
		Path nonExistingFileTemp = FileUtils.createTempFilePath(targetFilePath);
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
			FileUtils.ensureFileExists(targetFilePath);
			assertTrue(targetFilePath.toFile().exists());
			FileUtils.deleteFile(lock);
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
