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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.janelia.saalfeldlab.n5.BlockReader;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
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

  public static final String ZARR_JSON_FILE = "zarr.json";

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

  static Gson registerGson(final GsonBuilder gsonBuilder) {

    return addTypeAdapters(gsonBuilder).create();
  }

  protected static GsonBuilder addTypeAdapters(final GsonBuilder gsonBuilder) {

    gsonBuilder.registerTypeAdapter(
        org.janelia.saalfeldlab.n5.DataType.class,
        new org.janelia.saalfeldlab.n5.DataType.JsonAdapter());
    gsonBuilder.registerTypeAdapter(Zarr3CodecPipeline.class, Zarr3CodecPipeline.jsonAdapter);
    gsonBuilder.registerTypeHierarchyAdapter(ChunkKeyEncoding.class, ChunkKeyEncoding.jsonAdapter);
    gsonBuilder.registerTypeAdapter(DefaultChunkKeyEncoding.class,
        DefaultChunkKeyEncoding.jsonAdapter);
    gsonBuilder.registerTypeAdapter(V2ChunkKeyEncoding.class, V2ChunkKeyEncoding.jsonAdapter);
    gsonBuilder.registerTypeHierarchyAdapter(ChunkGrid.class, ChunkGrid.jsonAdapter);
    gsonBuilder.registerTypeAdapter(RegularChunkGrid.class, RegularChunkGrid.jsonAdapter);
    gsonBuilder.registerTypeAdapter(Zarr3Attributes.class, Zarr3Attributes.jsonAdapter);
    gsonBuilder.registerTypeAdapter(Zarr3DatasetAttributes.class,
        Zarr3DatasetAttributes.jsonAdapter);
    gsonBuilder.registerTypeAdapter(Zarr3GroupAttributes.class, Zarr3GroupAttributes.jsonAdapter);
    gsonBuilder.disableHtmlEscaping();
    gsonBuilder.serializeNulls();

    return gsonBuilder;
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
      return loadAttributes("").getZarrFormat();
    }
    return Zarr3Attributes.ZARR_3;
  }

  @Override
  public boolean exists(final String pathName) {
    final String normalPathName = N5URI.normalizeGroupPath(pathName);
    // Note that datasetExists and groupExists use the cache
    return groupExists(normalPathName) || datasetExists(normalPathName);
  }

  @Override
  public boolean groupExists(final String pathName) {
    final String normalPath = N5URI.normalizeGroupPath(pathName);
    if (cacheMeta()) {
      return cache.isGroup(normalPath, ZARR_JSON_FILE);
    }
    return isGroupFromContainer(normalPath);
  }

  @Override
  public boolean isGroupFromContainer(final String normalPath) {
    JsonElement attributes = getAttributesFromContainer(normalPath, ZARR_JSON_FILE);
    if (attributes != null) {
      return isGroupFromAttributes(ZARR_JSON_FILE, attributes);
    } else {
      return false;
    }
  }

  @Override
  public boolean isGroupFromAttributes(final String normalCacheKey, final JsonElement attributes) {
    if (normalCacheKey.equals(ZARR_JSON_FILE) && attributes != null && attributes.isJsonObject()) {
      try {
        return Zarr3GroupAttributes.fromJson(attributes, gson) != null;
      } catch (JsonParseException e) {
        return false;
      }
    } else {
      return false;
    }
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
  public boolean isDatasetFromContainer(final String normalPath) throws N5Exception {
    JsonElement attributes = getAttributesFromContainer(normalPath, ZARR_JSON_FILE);
    if (attributes != null) {
      return isDatasetFromAttributes(ZARR_JSON_FILE, attributes);
    } else {
      return false;
    }
  }

  @Override
  public boolean isDatasetFromAttributes(final String normalCacheKey,
      final JsonElement attributes) {
    if (normalCacheKey.equals(ZARR_JSON_FILE) && attributes != null && attributes.isJsonObject()) {
      try {
        return Zarr3DatasetAttributes.fromJson(attributes, gson) != null;
      } catch (JsonParseException e) {
        return false;
      }
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
    return (Zarr3DatasetAttributes) loadAttributes(pathName);
  }

  /**
   * Constructs {@link Zarr3DatasetAttributes} from a {@link JsonElement}.
   *
   * @param attributes the json element
   * @return the zarr array attributes
   */
  @Override
  public Zarr3DatasetAttributes createDatasetAttributes(final JsonElement attributes) {
    return Zarr3DatasetAttributes.fromJson(attributes, gson);
  }

  @Override
  public <T> T getAttribute(final String pathName, final String key, final Class<T> clazz)
      throws N5Exception {

    try {
      return (T) loadAttributes(pathName).getAttribute(key, gson, clazz);
    } catch (ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
  }

  @Override
  public <T> T getAttribute(final String pathName, final String key, final Type type)
      throws N5Exception {

    try {
      return (T) loadAttributes(pathName).getAttribute(key, gson, type);
    } catch (ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
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
    return loadAttributes(path).asN5Attributes(gson);
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

  protected Zarr3Attributes loadAttributes(String path) {
    String normalPath = N5URI.normalizeGroupPath(path);
    JsonElement json = cacheMeta() ? cache.getAttributes(normalPath, ZARR_JSON_FILE)
        : getAttributesFromContainer(normalPath, ZARR_JSON_FILE);
    if (json == null) {
      return null;
    }
    return Zarr3Attributes.fromJson(json.getAsJsonObject(), gson);
  }
}
