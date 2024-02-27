/**
 * Copyright (c) 2017--2021, Stephan Saalfeld All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer. 2. Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.CompressionAdapter;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
import org.janelia.saalfeldlab.n5.zarr.ZArrayAttributes;
import org.janelia.saalfeldlab.n5.zarr3.ChunkGrid.RegularChunkGrid;
import org.janelia.saalfeldlab.n5.zarr3.ChunkKeyEncoding.DefaultChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr3.ChunkKeyEncoding.V2ChunkKeyEncoding;
import org.janelia.saalfeldlab.n5.zarr3.cache.Zarr3JsonCache;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON attributes parsed with
 * {@link Gson}.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class Zarr3KeyValueReader implements CachedGsonKeyValueN5Reader, N5JsonCacheableContainer {

  public static final Version VERSION_ZERO = new Version(0, 0, 0);
  public static final Version VERSION = new Version(3, 0, 0);
  public static final String ZARR_FORMAT_KEY = "zarr_format";

  public static final String ZARR_JSON_FILE = "zarr.json";

  public static final String NODE_TYPE_KEY = "node_type";
  public static final String NODE_TYPE_GROUP = "group";
  public static final String NODE_TYPE_ARRAY = "array";

  protected final KeyValueAccess keyValueAccess;

  protected final Gson gson;

  protected final Zarr3JsonCache cache;

  protected final boolean cacheMeta;
  final protected boolean mapN5DatasetAttributes;
  protected URI uri;

  /**
   * Opens an {@link Zarr3KeyValueReader} at a given base path with a custom {@link GsonBuilder} to
   * support custom attributes.
   *
   * @param checkVersion           perform version check
   * @param keyValueAccess
   * @param basePath               N5 base path
   * @param gsonBuilder            the gson builder
   * @param mapN5DatasetAttributes If true, getAttributes and variants of getAttribute methods will
   *                               contain keys used by n5 datasets, and whose values are those for
   *                               their corresponding zarr fields. For example, if true, the key
   *                               "dimensions" (from n5) may be used to obtain the value of the key
   *                               "shape" (from zarr).
   * @param cacheMeta              cache attributes and meta data Setting this to true avoids
   *                               frequent reading and parsing of JSON encoded attributes and other
   *                               meta data that requires accessing the store. This is most
   *                               interesting for high latency backends. Changes of cached
   *                               attributes and meta data by an independent writer will not be
   *                               tracked.
   * @throws N5Exception if the base path cannot be read or does not exist, if the N5 version of the
   *                     container is not compatible with this implementation.
   */
  public Zarr3KeyValueReader(
      final boolean checkVersion,
      final KeyValueAccess keyValueAccess,
      final String basePath,
      final GsonBuilder gsonBuilder,
      final boolean mapN5DatasetAttributes,
      final boolean cacheMeta)
      throws N5Exception {

    this.keyValueAccess = keyValueAccess;
    this.gson = registerGson(gsonBuilder);
    this.cacheMeta = cacheMeta;
    this.mapN5DatasetAttributes = mapN5DatasetAttributes;

    try {
      uri = keyValueAccess.uri(basePath);
    } catch (final URISyntaxException e) {
      throw new N5Exception(e);
    }

    if (cacheMeta)
    // note normalExists isn't quite the normal version of exists.
    // rather, it only checks for the existence of the requested on the
    // backend
    // (this is the desired behavior the cache needs
    {
      cache = newCache();
    } else {
      cache = null;
    }
  }

  /**
   * Opens an {@link Zarr3KeyValueReader} at a given base path with a custom {@link GsonBuilder} to
   * support custom attributes.
   *
   * @param keyValueAccess
   * @param basePath       N5 base path
   * @param gsonBuilder
   * @param cacheMeta      cache attributes and meta data Setting this to true avoids frequent
   *                       reading and parsing of JSON encoded attributes and other meta data that
   *                       requires accessing the store. This is most interesting for high latency
   *                       backends. Changes of cached attributes and meta data by an independent
   *                       writer will not be tracked.
   * @throws N5Exception if the base path cannot be read or does not exist, if the N5 version of the
   *                     container is not compatible with this implementation.
   */
  public Zarr3KeyValueReader(
      final KeyValueAccess keyValueAccess,
      final String basePath,
      final GsonBuilder gsonBuilder,
      final boolean mapN5DatasetAttributes,
      final boolean cacheMeta)
      throws N5Exception {

    this(true, keyValueAccess, basePath, gsonBuilder, mapN5DatasetAttributes,
        cacheMeta);
  }

  /**
   * Reads a {@link DataBlock} from an {@link InputStream}.
   *
   * @param in
   * @param datasetAttributes
   * @param gridPosition
   * @return
   * @throws IOException
   */
  @SuppressWarnings("incomplete-switch")
  protected static DataBlock<?> readBlock(
      final InputStream in,
      final Zarr3DatasetAttributes datasetAttributes,
      final long... gridPosition) throws IOException {

    final int[] blockSize = datasetAttributes.getBlockSize();
    final DataType dataType = datasetAttributes.getDType();

    final ByteArrayDataBlock byteBlock = dataType.createByteBlock(blockSize, gridPosition);
    final BlockReader reader = datasetAttributes.getCompression().getReader();

    reader.read(byteBlock, in);

    switch (dataType.getDataType()) {
      case UINT8:
      case INT8:
        return byteBlock;
    }

    /* else translate into target type */
    final DataBlock<?> dataBlock = dataType.createDataBlock(blockSize, gridPosition);
    final ByteBuffer byteBuffer = byteBlock.toByteBuffer();
    dataBlock.readData(byteBuffer);

    return dataBlock;
  }

  protected static Version getVersion(final JsonElement json) {

    if (json == null || !json.isJsonObject()) {
      return VERSION_ZERO;
    }

    final JsonElement fmt = json.getAsJsonObject().get(ZARR_FORMAT_KEY);
    if (fmt.isJsonPrimitive()) {
      return new Version(fmt.getAsInt(), 0, 0);
    }

    return null;
  }

  static Gson registerGson(final GsonBuilder gsonBuilder) {

    return addTypeAdapters(gsonBuilder).create();
  }

  protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

    gsonBuilder.registerTypeAdapter(
        org.janelia.saalfeldlab.n5.DataType.class,
        new org.janelia.saalfeldlab.n5.DataType.JsonAdapter());
    gsonBuilder.registerTypeAdapter(Zarr3CodecPipeline.class, Zarr3CodecPipeline.jsonAdapter);
    gsonBuilder.registerTypeHierarchyAdapter(ChunkKeyEncoding.class, ChunkKeyEncoding.jsonAdapter);
    gsonBuilder.registerTypeAdapter(DefaultChunkKeyEncoding.class, DefaultChunkKeyEncoding.jsonAdapter);
    gsonBuilder.registerTypeAdapter(V2ChunkKeyEncoding.class, V2ChunkKeyEncoding.jsonAdapter);
    gsonBuilder.registerTypeHierarchyAdapter(ChunkGrid.class, ChunkGrid.jsonAdapter);
    gsonBuilder.registerTypeAdapter(RegularChunkGrid.class, RegularChunkGrid.jsonAdapter);
    gsonBuilder.registerTypeAdapter(Zarr3ArrayAttributes.class, Zarr3ArrayAttributes.jsonAdapter);
    gsonBuilder.disableHtmlEscaping();
    gsonBuilder.serializeNulls();

    return gsonBuilder;
  }

  protected static JsonElement reverseAttrsWhenCOrder(final JsonElement elem) {
    return elem;
    /*if (elem == null || !elem.isJsonObject()) {
      return elem;
    }

    final JsonObject attrs = elem.getAsJsonObject();
    final JsonArray shape = attrs.get(ZArrayAttributes.shapeKey).getAsJsonArray();
    Zarr3KeyValueWriter.reorder(shape);
    attrs.add(ZArrayAttributes.shapeKey, shape);

    final JsonArray chunkSize = attrs.get(ZArrayAttributes.chunksKey).getAsJsonArray();
    Zarr3KeyValueWriter.reorder(chunkSize);
    attrs.add(ZArrayAttributes.chunksKey, chunkSize);
    return attrs;*/
  }

  @Override
  public Gson getGson() {

    return gson;
  }

  @Override
  public Zarr3JsonCache newCache() {

    return new Zarr3JsonCache(this);
  }

  @Override
  public boolean cacheMeta() {

    return cacheMeta;
  }

  @Override
  public Zarr3JsonCache getCache() {

    return cache;
  }

  @Override
  public KeyValueAccess getKeyValueAccess() {

    return keyValueAccess;
  }

  @Override
  public URI getURI() {

    return uri;
  }

  /**
   * Returns the version of this zarr container.
   *
   * @return the {@link Version}
   */
  @Override
  public Version getVersion() throws N5Exception {
    if (exists("")) {
      return getVersion(getZarrJson(""));
    }
    return VERSION;
  }

  @Override
  public boolean exists(final String pathName) {

    // Overridden because of the difference in how n5 and zarr define "group" and "dataset".
    // The implementation in CachedGsonKeyValueReader is simpler but more low-level
    final String normalPathName = N5URI.normalizeGroupPath(pathName);

    // Note that datasetExists and groupExists use the cache
    return groupExists(normalPathName) || datasetExists(normalPathName);
  }

  @Override
  public boolean groupExists(final String pathName) {

    // Overriden because the parent implementation uses attributes.json,
    // this uses .zgroup.
    final String normalPath = N5URI.normalizeGroupPath(pathName);
    if (cacheMeta()) {
      return cache.isGroup(normalPath, ZARR_JSON_FILE);
    }
    return isGroupFromContainer(normalPath);
  }

  @Override
  public boolean isGroupFromContainer(final String normalPath) {
    return keyValueAccess.isFile(keyValueAccess.compose(uri, normalPath, ZARR_JSON_FILE));
  }

  @Override
  public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {

    return attributes != null && attributes.isJsonObject() && attributes.getAsJsonObject()
        .has(ZARR_FORMAT_KEY);
  }

  @Override
  public boolean datasetExists(final String pathName) throws N5IOException {

    if (cacheMeta()) {
      final String normalPathName = N5URI.normalizeGroupPath(pathName);
      return cache.isDataset(normalPathName, ZARR_JSON_FILE);
    }
    return isDatasetFromContainer(pathName);
  }

  @Override
  public boolean isDatasetFromContainer(final String normalPathName) throws N5Exception {

    if (keyValueAccess.isFile(keyValueAccess.compose(uri, normalPathName, ZARR_JSON_FILE))) {
      return isDatasetFromAttributes(ZARR_JSON_FILE,
          getAttributesFromContainer(normalPathName, ZARR_JSON_FILE));
    } else {
      return false;
    }
  }

  @Override
  public boolean isDatasetFromAttributes(final String normalCacheKey,
      final JsonElement attributes) {

    if (normalCacheKey.equals(ZARR_JSON_FILE) && attributes != null && attributes.isJsonObject()) {
      return createDatasetAttributes(attributes) != null;
    } else {
      return false;
    }
  }

  /**
   * Returns the {@link Zarr3DatasetAttributes} located at the given path, if present.
   *
   * @param pathName the path relative to the container's root
   * @return the zarr array attributes
   * @throws N5Exception the exception
   */
  @Override
  public Zarr3DatasetAttributes getDatasetAttributes(final String pathName) throws N5Exception {

    return createDatasetAttributes(getZarrJson(pathName));
  }

  /**
   * Returns the {@link Zarr3ArrayAttributes} located at the given path, if present.
   *
   * @param pathName the path relative to the container's root
   * @return the zarr array attributes
   * @throws N5Exception the exception
   */
  public Zarr3ArrayAttributes getArrayAttributes(final String pathName) throws N5Exception {

    return getArrayAttributes(getZarrJson(pathName));
  }

  /**
   * Constructs {@link Zarr3ArrayAttributes} from a {@link JsonElement}.
   *
   * @param attributes the json element
   * @return the zarr array attributes
   */
  protected Zarr3ArrayAttributes getArrayAttributes(final JsonElement attributes) {

    return gson.fromJson(attributes, Zarr3ArrayAttributes.class);
  }


  /**
   * Returns the {@link Zarr3GroupAttributes} located at the given path, if present.
   *
   * @param pathName the path relative to the container's root
   * @return the zarr array attributes
   * @throws N5Exception the exception
   */
  public Zarr3GroupAttributes getGroupAttributes(final String pathName) throws N5Exception {

    return getGroupAttributes(getZarrJson(pathName));
  }

  /**
   * Constructs {@link Zarr3GroupAttributes} from a {@link JsonElement}.
   *
   * @param attributes the json element
   * @return the zarr array attributes
   */
  protected Zarr3GroupAttributes getGroupAttributes(final JsonElement attributes) {

    return gson.fromJson(attributes, Zarr3GroupAttributes.class);
  }

  @Override
  public Zarr3DatasetAttributes createDatasetAttributes(final JsonElement attributes) {

    final Zarr3ArrayAttributes zarray = getArrayAttributes(attributes);
    return zarray != null ? zarray.getDatasetAttributes() : null;
  }

  @Override
  public <T> T getAttribute(final String pathName, final String key, final Class<T> clazz)
      throws N5Exception {

    final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
    final JsonElement attributes = getAttributes(pathName); // handles caching
    try {
      return GsonUtils.readAttribute(attributes, normalizedAttributePath, clazz, gson);
    } catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
  }

  @Override
  public <T> T getAttribute(final String pathName, final String key, final Type type)
      throws N5Exception {

    final String normalizedAttributePath = N5URI.normalizeAttributePath(key);
    final JsonElement attributes = getAttributes(pathName); // handles caching
    try {
      return GsonUtils.readAttribute(attributes, normalizedAttributePath, type, gson);
    } catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
  }

  /**
   * Returns a {@link JsonElement} representing the contents of a JSON file located at the given
   * path if present, and null if not.  Also updates the cache.
   *
   * @param normalPath the normalized path
   * @param jsonName   the name of the JSON file
   * @return the JSON element
   * @throws N5Exception the exception
   */
  protected JsonElement getJsonResource(final String normalPath, final String jsonName)
      throws N5Exception {

    if (cacheMeta()) {
      return cache.getAttributes(normalPath, jsonName);
    } else {
      return getAttributesFromContainer(normalPath, jsonName);
    }
  }

  /**
   * Returns a {@link JsonElement} representing the contents of zarr.json located at the given path
   * if present, and null if not.
   *
   * @param path the path
   * @return the JSON element
   * @throws N5Exception the exception
   */
  protected JsonElement getZarrJson(final String path) throws N5Exception {
    return getJsonResource(N5URI.normalizeGroupPath(path), ZARR_JSON_FILE);
  }

  protected JsonElement zarrToN5DatasetAttributes(final JsonElement elem) {

    if (!mapN5DatasetAttributes || elem == null || !elem.isJsonObject()) {
      return elem;
    }

    final JsonObject attrs = elem.getAsJsonObject();
    final Zarr3ArrayAttributes zattrs = getArrayAttributes(attrs);
    if (zattrs == null) {
      return elem;
    }

    attrs.add(DatasetAttributes.DIMENSIONS_KEY, attrs.get(Zarr3ArrayAttributes.shapeKey));
    attrs.add(DatasetAttributes.BLOCK_SIZE_KEY,
        attrs.get(Zarr3ArrayAttributes.chunkGridKey).getAsJsonObject().get("configuration")
            .getAsJsonObject().get("chunk_shape"));
    attrs.addProperty(DatasetAttributes.DATA_TYPE_KEY, zattrs.getDType().getDataType().toString());

    // TODO: Codecs
    final JsonElement e = attrs.get(Zarr3ArrayAttributes.codecsKey);
    if (e == JsonNull.INSTANCE) {
      attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(new RawCompression()));
    } else {
      attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(
          gson.fromJson(attrs.get(Zarr3ArrayAttributes.codecsKey), ZarrCodec.class)
              .getCompression()));
    }
    return attrs;
  }

  protected JsonElement n5ToZarrDatasetAttributes(final JsonElement elem) {

    if (!mapN5DatasetAttributes || elem == null || !elem.isJsonObject()) {
      return elem;
    }

    final JsonObject attrs = elem.getAsJsonObject();
    if (attrs.has(DatasetAttributes.DIMENSIONS_KEY)) {
      attrs.add(Zarr3ArrayAttributes.shapeKey, attrs.get(DatasetAttributes.DIMENSIONS_KEY));
    }

    if (attrs.has(DatasetAttributes.BLOCK_SIZE_KEY)) {
      JsonObject chunkGridConfigurationObject = new JsonObject();
      chunkGridConfigurationObject.add("chunk_shape", attrs.get(DatasetAttributes.BLOCK_SIZE_KEY));
      JsonObject chunkGridObject = new JsonObject();
      chunkGridObject.add("name", new JsonPrimitive("regular"));
      chunkGridObject.add("configuration", chunkGridConfigurationObject);
      attrs.add(Zarr3ArrayAttributes.chunkGridKey, chunkGridObject);
    }

    if (attrs.has(DatasetAttributes.DATA_TYPE_KEY)) {
      attrs.add(Zarr3ArrayAttributes.dataTypeKey, attrs.get(DatasetAttributes.DATA_TYPE_KEY));
    }

    return attrs;
  }

  /**
   * Returns the attributes at the given path.
   *
   * @param path the path
   * @return the json element
   * @throws N5Exception the exception
   */
  @Override
  public JsonElement getAttributes(final String path) throws N5Exception {
    return zarrToN5DatasetAttributes(reverseAttrsWhenCOrder(getZarrJson(path)));
  }

  protected JsonElement getAttributesUnmapped(final String path) throws N5Exception {
    return reverseAttrsWhenCOrder(getZarrJson(path));
  }

  @Override
  public JsonElement getAttributesFromContainer(
      final String normalResourceParent,
      final String normalResourcePath) throws N5Exception {

    final String absolutePath = keyValueAccess.compose(uri, normalResourceParent,
        normalResourcePath);
    if (!keyValueAccess.exists(absolutePath)) {
      return null;
    }

    try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
      return GsonUtils.readAttributes(lockedChannel.newReader(), gson);
    } catch (final IOException | UncheckedIOException e) {
      throw new N5IOException("Failed to read " + absolutePath, e);
    }
  }

  @Override
  public DataBlock<?> readBlock(
      final String pathName,
      final DatasetAttributes datasetAttributes,
      final long... gridPosition) throws N5Exception {

    final Zarr3DatasetAttributes zarrDatasetAttributes;
    if (datasetAttributes instanceof Zarr3DatasetAttributes) {
      zarrDatasetAttributes = (Zarr3DatasetAttributes) datasetAttributes;
    } else {
      zarrDatasetAttributes = getDatasetAttributes(pathName);
    }

    final String absolutePath = keyValueAccess
        .compose(
            uri,
            pathName,
            zarrDatasetAttributes.getChunkKeyEncoding().encodeChunkKey(gridPosition));

    if (!keyValueAccess.exists(absolutePath)) {
      return null;
    }

    try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(absolutePath)) {
      return readBlock(lockedChannel.newInputStream(), zarrDatasetAttributes, gridPosition);
    } catch (final Throwable e) {
      throw new N5IOException(
          "Failed to read block " + Arrays.toString(gridPosition) + " from dataset " + pathName,
          e);
    }
  }

  @Override
  public String toString() {

    return String.format("%s[access=%s, basePath=%s]", getClass().getSimpleName(), keyValueAccess,
        uri.getPath());
  }

}
