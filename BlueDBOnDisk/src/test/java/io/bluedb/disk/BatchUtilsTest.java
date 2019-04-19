package io.bluedb.disk;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.api.keys.TimeKey;
import io.bluedb.disk.recovery.IndividualChange;
import io.bluedb.disk.segment.Range;
import io.bluedb.disk.segment.Segment;

public class BatchUtilsTest {

	private static BlueKey key0At0 = new TimeKey(0, 0);
	private static BlueKey key1At1To5 = new TimeFrameKey(1, 1, 5);
	private static BlueKey key4At4 = new TimeKey(4, 4);
	private static TestValue value0 = new TestValue("0");
	private static TestValue value1 = new TestValue("1");
	private static TestValue value4 = new TestValue("4");
	private static IndividualChange<TestValue> insert0At0 = IndividualChange.createInsertChange(key0At0, value0);
	private static IndividualChange<TestValue> insert1At1To5 = IndividualChange.createInsertChange(key1At1To5, value1);
	private static IndividualChange<TestValue> insert4At4 = IndividualChange.createInsertChange(key4At4, value4);
	private static List<IndividualChange<TestValue>> empty = Arrays.asList();
	private static List<IndividualChange<TestValue>> inserts0and1to5and4 = Arrays.asList(insert0At0, insert1At1To5, insert4At4);
	private static List<IndividualChange<TestValue>> inserts1to5and4 = Arrays.asList(insert1At1To5, insert4At4);
	private static List<IndividualChange<TestValue>> inserts1to5 = Arrays.asList(insert1At1To5);

	@Test
	public void test_constructor() { // just to maintain 100% coverage
		new BatchUtils();
	}

	@Test
	public void test_removeChangesThatEndInOrBeforeSegment() {
		LinkedList<IndividualChange<TestValue>> list;

		list = new LinkedList<>(inserts0and1to5and4); 
		BatchUtils.removeChangesThatEndInOrBeforeSegment(list, segmentEnding(-1));
		assertEquals(inserts0and1to5and4, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		BatchUtils.removeChangesThatEndInOrBeforeSegment(list, segmentEnding(0));
		assertEquals(inserts1to5and4, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		BatchUtils.removeChangesThatEndInOrBeforeSegment(list, segmentEnding(1));
		assertEquals(inserts1to5and4, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		BatchUtils.removeChangesThatEndInOrBeforeSegment(list, segmentEnding(4));
		assertEquals(inserts1to5, list);

		list = new LinkedList<>(inserts0and1to5and4); 
		BatchUtils.removeChangesThatEndInOrBeforeSegment(list, segmentEnding(5));
		assertEquals(empty, list);
	}

	private static Segment<TestValue> segmentEnding(long end) {
		Range range = new Range(end, end);
		return new Segment<TestValue>(null, range, null, null, null);
	}
}
