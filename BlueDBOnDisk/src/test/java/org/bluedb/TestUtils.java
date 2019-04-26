package org.bluedb;

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
}
