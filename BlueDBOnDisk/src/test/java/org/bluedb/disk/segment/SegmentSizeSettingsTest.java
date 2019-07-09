package org.bluedb.disk.segment;

import static java.util.Arrays.asList;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.FIFTEEN_DAYS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.FIVE_DAYS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_DAY;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_HOUR;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_MILLI;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.ONE_MONTH;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_HOURS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_MONTHS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.SIX_SECONDS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.THREE_MONTHS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.TWELVE_HOURS;
import static org.bluedb.disk.segment.path.SegmentPathTimeUnits.TWO_HOURS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.TestUtils;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.HashGroupedKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.StringKey;
import org.bluedb.api.keys.TimeFrameKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.segment.path.SegmentSizeConfiguration;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;
import org.junit.Test;

public class SegmentSizeSettingsTest {
	private static final List<SegmentSizeConfiguration> allSupportedTimeKeyConfigs = Arrays.asList(
		new SegmentSizeConfiguration(TimeKey.class, 	asList(ONE_HOUR, 	24L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(TWO_HOURS, 	12L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, TWO_HOURS)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(SIX_HOURS, 	 4L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, SIX_HOURS)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(TWELVE_HOURS, 2L, 30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, TWELVE_HOURS)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(ONE_DAY, 	     30L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(FIVE_DAYS, 		  6L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, FIVE_DAYS)),
		new SegmentSizeConfiguration(TimeKey.class,		asList(FIFTEEN_DAYS, 	  2L, 12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, FIFTEEN_DAYS)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(ONE_MONTH, 			  12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, ONE_MONTH)),
		new SegmentSizeConfiguration(TimeKey.class,		asList(THREE_MONTHS, 		  12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, ONE_MONTH, THREE_MONTHS)),
		new SegmentSizeConfiguration(TimeKey.class, 	asList(SIX_MONTHS, 			  12L), asList(ONE_MILLI, SIX_SECONDS, ONE_HOUR, ONE_DAY, ONE_MONTH, SIX_MONTHS))
	);
	
	private static final List<SegmentSizeConfiguration> allSupportedIntegerKeyConfigs = Arrays.asList(
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(128L,  128L, 64L, 64L), 	asList(1L, 128L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(256L,   64L, 64L, 64L), 	asList(1L, 256L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(512L,   32L, 64L, 64L), 	asList(1L, 512L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(1024L,  16L, 64L, 64L), 	asList(1L, 1024L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(2048L,   8L, 64L, 64L), 	asList(1L, 2048L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(4096L,   4L, 64L, 64L), 	asList(1L, 4096L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(8192L,   2L, 64L, 64L), 	asList(1L, 8192L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(16384L,      64L, 64L), 	asList(1L, 16384L)),
		new SegmentSizeConfiguration(IntegerKey.class, 	asList(32768L,      32L, 64L), 	asList(1L, 32768L))
	);
	
	private static final List<SegmentSizeConfiguration> allSupportedLongKeyConfigs = Arrays.asList(
		new SegmentSizeConfiguration(LongKey.class, 	asList(64L,   256L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 64L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(128L,  128L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 128L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(256L,   64L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 256L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(512L,   32L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 512L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(1024L,  16L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 1024L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(2048L,   8L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 2048L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(4096L,   4L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 4096L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(8192L,   2L, 512L, 512L, 512L, 256L, 128L), 	asList(1L, 8192L)),
		new SegmentSizeConfiguration(LongKey.class, 	asList(16384L,      512L, 512L, 512L, 256L, 128L), 	asList(1L, 16384L))
	);
	
	private static final List<SegmentSizeConfiguration> allSupportedHashKeyConfigs = Arrays.asList(
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(1024L,    512L, 128L, 64L), 	asList(1L, 1024L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(2048L,    256L, 128L, 64L), 	asList(1L, 2048L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(4096L,    128L, 128L, 64L), 	asList(1L, 4096L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(8192L,     64L, 128L, 64L),  asList(1L, 8192L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(16384L,    32L, 128L, 64L),  asList(1L, 16384L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(32768L,    16L, 128L, 64L),  asList(1L, 32768L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(65536L,     8L, 128L, 64L),  asList(1L, 65536L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(131072L,    4L, 128L, 64L),  asList(1L, 131072L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(262144L,    2L, 128L, 64L),  asList(1L, 262144L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(524288L,        128L, 64L),  asList(1L, 524288L)),
		new SegmentSizeConfiguration(HashGroupedKey.class,	asList(1048576L,        64L, 64L),  asList(1L, 1048576L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(2097152L,        32L, 64L),  asList(1L, 2097152L)),
		new SegmentSizeConfiguration(HashGroupedKey.class,	asList(4194304L,        16L, 64L),  asList(1L, 4194304L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(8388608L,         8L, 64L),	asList(1L, 8388608L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(16777216L,        4L, 64L),  asList(1L, 16777216L)),
		new SegmentSizeConfiguration(HashGroupedKey.class, 	asList(33554432L,        2L, 64L),  asList(1L, 33554432L))
	);

	private static List<SegmentSizeConfiguration> getAllConfigs() {
		List<SegmentSizeConfiguration> allConfigs = new LinkedList<>();
		allConfigs.addAll(allSupportedTimeKeyConfigs);
		allConfigs.addAll(allSupportedIntegerKeyConfigs);
		allConfigs.addAll(allSupportedLongKeyConfigs);
		allConfigs.addAll(allSupportedHashKeyConfigs);
		return allConfigs;
	}
	
	@Test
	public void testGetDefaultIndexSettingsFor() throws Exception {
		assertEquals(SegmentSizeSetting.TIME_1_DAY, SegmentSizeSetting.getDefaultIndexSettingsFor(TimeKey.class));
		assertEquals(SegmentSizeSetting.TIME_1_DAY, SegmentSizeSetting.getDefaultIndexSettingsFor(TimeFrameKey.class));
		assertEquals(SegmentSizeSetting.LONG_512, SegmentSizeSetting.getDefaultIndexSettingsFor(LongKey.class));
		assertEquals(SegmentSizeSetting.INT_512, SegmentSizeSetting.getDefaultIndexSettingsFor(IntegerKey.class));
		assertEquals(SegmentSizeSetting.HASH_2M, SegmentSizeSetting.getDefaultIndexSettingsFor(StringKey.class));
		assertEquals(SegmentSizeSetting.HASH_2M, SegmentSizeSetting.getDefaultIndexSettingsFor(UUIDKey.class));
		
		try {
			SegmentSizeSetting.getDefaultIndexSettingsFor(BlueKey.class);
			fail();
		} catch (Exception e) {}
	}

	@Test
	public void testGetDefaultSettingsFor() throws Exception {
		assertEquals(SegmentSizeSetting.TIME_1_HOUR, SegmentSizeSetting.getDefaultSettingsFor(TimeKey.class));
		assertEquals(SegmentSizeSetting.TIME_1_HOUR, SegmentSizeSetting.getDefaultSettingsFor(TimeFrameKey.class));
		assertEquals(SegmentSizeSetting.LONG_256, SegmentSizeSetting.getDefaultSettingsFor(LongKey.class));
		assertEquals(SegmentSizeSetting.INT_256, SegmentSizeSetting.getDefaultSettingsFor(IntegerKey.class));
		assertEquals(SegmentSizeSetting.HASH_1M, SegmentSizeSetting.getDefaultSettingsFor(StringKey.class));
		assertEquals(SegmentSizeSetting.HASH_1M, SegmentSizeSetting.getDefaultSettingsFor(UUIDKey.class));
		
		try {
			SegmentSizeSetting.getDefaultSettingsFor(BlueKey.class);
			fail();
		} catch (Exception e) {}
	}

	@Test
	public void testGetOriginalDefaultSettingsFor() throws Exception {
		assertEquals(SegmentSizeSetting.TIME_1_HOUR, SegmentSizeSetting.getOriginalDefaultSettingsFor(TimeKey.class));
		assertEquals(SegmentSizeSetting.TIME_1_HOUR, SegmentSizeSetting.getOriginalDefaultSettingsFor(TimeFrameKey.class));
		assertEquals(SegmentSizeSetting.LONG_128, SegmentSizeSetting.getOriginalDefaultSettingsFor(LongKey.class));
		assertEquals(SegmentSizeSetting.INT_256, SegmentSizeSetting.getOriginalDefaultSettingsFor(IntegerKey.class));
		assertEquals(SegmentSizeSetting.HASH_512K, SegmentSizeSetting.getOriginalDefaultSettingsFor(StringKey.class));
		assertEquals(SegmentSizeSetting.HASH_512K, SegmentSizeSetting.getOriginalDefaultSettingsFor(UUIDKey.class));

		try {
			SegmentSizeSetting.getOriginalDefaultSettingsFor(BlueKey.class);
			fail();
		} catch (Exception e) {}
	}
	
	@Test
	public void testCoverageOfCurrentSegmentSizeSettings() {
		List<SegmentSizeConfiguration> allConfigs = getAllConfigs();
		
		for(SegmentSizeSetting setting : SegmentSizeSetting.values()) {
			boolean configContainedInTestData = allConfigs.contains(setting.getConfig());
			assertTrue("Segment Size Setting not covered in tests: " + setting, configContainedInTestData);
		}
	}
	
	@Test
	public void testSettingDelegates() {
		for(SegmentSizeSetting setting : SegmentSizeSetting.values()) {
			SegmentSizeConfiguration config = setting.getConfig();
			
			assertEquals(config.getSegmentSize(), setting.getSegmentSize());
			assertEquals(config.getRollupsBottomToTop(), setting.getRollupSizes());
			assertEquals(config.getFolderSizesTopToBottom(), setting.getFolderSizes());
		}
	}
	
	@Test
	public void testFolderSizes() {
		for(SegmentSizeConfiguration config : getAllConfigs()) {
			List<Long> folderSizes = config.getFolderSizesTopToBottom();
			for (int i = 0; i < folderSizes.size() - 1; i++) {
				String errorMessage = "Config has invalid folder sizes [size" + (i+1) +"]" + folderSizes.get(i+1) + " [size" + i + "]" + folderSizes.get(i) + " [config]" + config;
				assertTrue(errorMessage, folderSizes.get(i) % folderSizes.get(i+1) == 0);
			}
		}
	}
	
	@Test
	public void testRollupSizes() {
		for(SegmentSizeConfiguration config : getAllConfigs()) {
			List<Long> rollupLevels = config.getRollupsBottomToTop();
			for (int i = 0; i < rollupLevels.size() - 1; i++) {
				String errorMessage = "Config has invalid folder sizes [size" + (i) +"]" + rollupLevels.get(i) + " [size" + (i+1) + "]" + rollupLevels.get(i+1) + " [config]" + config;
				assertTrue(errorMessage, rollupLevels.get(i + 1) % rollupLevels.get(i) == 0);
			}
			
			assertEquals((long)config.getSegmentSize(), (long)rollupLevels.get(rollupLevels.size()-1));
		}
	}
	
	@Test
	public void testDeserialization() throws URISyntaxException, IOException, SerializationException {
		Path serializedSettingsPath = TestUtils.getResourcePath("segment_size_settings.bin");
		BlueSerializer serializer = new ThreadLocalFstSerializer();
		
		List<SegmentSizeSetting> allValuesInCodeInAlphabeticalOrder = Arrays.asList(SegmentSizeSetting.values());
		Collections.sort(allValuesInCodeInAlphabeticalOrder, Comparator.comparing(SegmentSizeSetting::name));
		
		if(Files.exists(serializedSettingsPath)) {
			@SuppressWarnings("unchecked")
			List<SegmentSizeSetting> allSerializedValues = (List<SegmentSizeSetting>) serializer.deserializeObjectFromByteArray(Files.readAllBytes(serializedSettingsPath));
			
			assertTrue("You can't remove constants from the SegmentSizeSettings enum without breaking serialization.", 
					allValuesInCodeInAlphabeticalOrder.size() >= allSerializedValues.size());
			
			List<SegmentSizeSetting> trimmedValuesInCode = new ArrayList<>(Arrays.asList(SegmentSizeSetting.values()));
			while(trimmedValuesInCode.size() > allSerializedValues.size()) {
				trimmedValuesInCode.remove(trimmedValuesInCode.size()-1);
			}
			Collections.sort(trimmedValuesInCode, Comparator.comparing(SegmentSizeSetting::name));
			
			assertEquals("Do not remove or change the order of existing enum constants.", 
					trimmedValuesInCode, allSerializedValues);
			
			assertEquals("Remember to put new enum constants under test by deleting the serialized test file and run again in order to re-create it.", 
					allSerializedValues, allValuesInCodeInAlphabeticalOrder);
		} else {
			Files.write(serializedSettingsPath, serializer.serializeObjectToByteArray(allValuesInCodeInAlphabeticalOrder));
			fail();
		}
	}
}
