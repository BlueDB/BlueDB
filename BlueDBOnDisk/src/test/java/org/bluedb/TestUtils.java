package org.bluedb;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtils {
	public static Path getResourcePath(String relativePath) throws URISyntaxException, IOException {
		Path pathToStartFrom = Paths.get(TestUtils.class.getResource("").toURI());
		while(!pathToStartFrom.endsWith("BlueDBOnDisk")) {
			pathToStartFrom = pathToStartFrom.resolve("../").toRealPath();
		}
		return pathToStartFrom.resolve("src/test/resources").resolve(relativePath);
	}

	public static void assertThrowable(Class<? extends Throwable> expected, Throwable error) {
		String message = "Expected throwable of type " + expected + " but was actually of type " + error;
		if(error == null) {
			if(expected != null) {
				fail(message);
			}
		}
		else if(!error.getClass().isAssignableFrom(expected)) {
			fail(message);
		}
	}
}
