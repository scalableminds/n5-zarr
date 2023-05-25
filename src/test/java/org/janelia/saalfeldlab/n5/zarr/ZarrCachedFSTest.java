package org.janelia.saalfeldlab.n5.zarr;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5CachedFSTest;
import org.janelia.saalfeldlab.n5.N5CachedFSTest.TrackingStorage;
import org.junit.Assert;
import org.junit.Test;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class ZarrCachedFSTest extends N5ZarrTest {

	@Override
	protected N5ZarrWriter createN5Writer() throws IOException {

		return createN5Writer(true);
	}

	protected N5ZarrWriter createN5Writer(final boolean cacheAttributes) throws IOException {

		return createN5Writer(tempN5PathName(), new GsonBuilder(), ".", cacheAttributes);
	}

	@Override
	protected N5ZarrWriter createN5Writer(String location, GsonBuilder gsonBuilder) throws IOException {

		return createN5Writer(location, gsonBuilder, ".");
	}

	protected N5ZarrWriter createN5Writer(String location, String dimensionSeparator) throws IOException {

		return createN5Writer(location, new GsonBuilder(), dimensionSeparator);
	}

	protected N5ZarrWriter createN5Writer(String location, GsonBuilder gsonBuilder, String dimensionSeparator, boolean cachedAttributes ) throws IOException {

		final Path testN5Path = Paths.get(location);
		final boolean existsBefore = testN5Path.toFile().exists();
		final N5ZarrWriter zarr = new N5ZarrWriter(location, gsonBuilder, dimensionSeparator, true, cachedAttributes);
		final boolean existsAfter = testN5Path.toFile().exists();
		if (!existsBefore && existsAfter) {
			tmpFiles.add(location);
		}
		return zarr;
	}

	protected static String tempN5PathName() {
		try {
			final File tmpFile = Files.createTempDirectory("zarr-cached-test-").toFile();
			tmpFile.deleteOnExit();
			final String tmpPath = tmpFile.getCanonicalPath();
			tmpFiles.add(tmpPath);
			return tmpPath;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void cacheTest() throws IOException {
		/* Test the cache by setting many attributes, then manually deleting the underlying file.
		* The only possible way for the test to succeed is if it never again attempts to read the file, and relies on the cache. */

		final String cachedGroup = "cachedGroup";
		try (ZarrKeyValueWriter zarr = (ZarrKeyValueWriter) createN5Writer()) {
			zarr.createGroup(cachedGroup);
			final String attributesPath = zarr.zAttrsAbsolutePath(cachedGroup);

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			runTests(zarr, tests);
		}

		try (ZarrKeyValueWriter zarr = (ZarrKeyValueWriter) createN5Writer(false)) {
			zarr.createGroup(cachedGroup);
			final String attributesPath = zarr.zAttrsAbsolutePath(cachedGroup);

			final ArrayList<TestData<?>> tests = new ArrayList<>();
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/b/c", 100));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[5]", "asdf"));
			addAndTest(zarr, tests, new TestData<>(cachedGroup, "a/a[2]", 0));

			Files.delete(Paths.get(attributesPath));
			Assert.assertThrows(AssertionError.class, () -> runTests(zarr, tests));
		}
	}

	@Test
	public void cacheBehaviorTest() throws IOException, URISyntaxException {

		final String loc = tempN5Location();
		// make an uncached n5 writer
		try (final ZarrTrackingStorage n5 = new ZarrTrackingStorage(
				new FileSystemKeyValueAccess(FileSystems.getDefault()), loc, new GsonBuilder(), true)) {

			N5CachedFSTest.cacheBehaviorHelper(n5);
		}
	}

	public static class ZarrTrackingStorage extends ZarrKeyValueWriter implements TrackingStorage {

		public int attrCallCount = 0;
		public int existsCallCount = 0;
		public int groupCallCount = 0;
		public int groupAttrCallCount = 0;
		public int datasetCallCount = 0;
		public int datasetAttrCallCount = 0;
		public int listCallCount = 0;

		public ZarrTrackingStorage(final KeyValueAccess keyValueAccess, final String basePath,
				final GsonBuilder gsonBuilder, final boolean cacheAttributes) throws IOException {

			super(keyValueAccess, basePath, gsonBuilder, true, true, ".", cacheAttributes);
		}

		public JsonElement getAttributesFromContainer(final String key, final String cacheKey) {
			attrCallCount++;
			return super.getAttributesFromContainer(key, cacheKey);
		}

		public boolean existsFromContainer(final String path, final String cacheKey) {
			existsCallCount++;
			return super.existsFromContainer(path, cacheKey);
		}

		public boolean isGroupFromContainer(final String key) {
			groupCallCount++;
			return super.isGroupFromContainer(key);
		}

		public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			groupAttrCallCount++;
			return super.isGroupFromAttributes(normalCacheKey, attributes);
		}

		public boolean isDatasetFromContainer(final String key) {
			datasetCallCount++;
			return super.isDatasetFromContainer(key);
		}

		public boolean isDatasetFromAttributes(final String normalCacheKey, final JsonElement attributes) {
			datasetAttrCallCount++;
			return super.isDatasetFromAttributes(normalCacheKey, attributes);
		}

		public String[] listFromContainer(final String key) {
			listCallCount++;
			return super.listFromContainer(key);
		}

		public int getAttrCallCount() {
			return attrCallCount;
		}

		public int getExistCallCount() {
			return existsCallCount;
		}

		public int getGroupCallCount() {
			return groupCallCount;
		}

		public int getGroupAttrCallCount() {
			return groupAttrCallCount;
		}

		public int getDatasetCallCount() {
			return datasetCallCount;
		}

		public int getDatasetAttrCallCount() {
			return datasetAttrCallCount;
		}

		public int getListCallCount() {
			return listCallCount;
		}
	}
}
