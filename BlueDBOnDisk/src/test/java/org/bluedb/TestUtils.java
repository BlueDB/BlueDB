package org.bluedb;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtils {
	public static Path getResourcePath(String relativePath) throws URISyntaxException {
		Path pathToStartFrom = Paths.get(TestUtils.class.getResource("").toURI());
		return pathToStartFrom.resolve("../../../../src/test/resources").resolve(relativePath);
	}
}
