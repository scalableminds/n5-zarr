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
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import net.imglib2.Cursor;
import net.imglib2.FinalInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.basictypeaccess.array.ByteArray;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.util.Intervals;
import net.imglib2.view.Views;
import org.janelia.saalfeldlab.n5.BlockWriter;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.zarr3.ChunkGrid.RegularChunkGrid;

/**
 * Zarr {@link KeyValueWriter} implementation.
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 */
public class Zarr3KeyValueWriter extends
    Zarr3KeyValueReader implements CachedGsonKeyValueN5Writer {

  protected ChunkKeyEncoding chunkKeyEncoding;

  /**
   * Opens an {@link Zarr3KeyValueWriter} at a given base path with a custom {@link GsonBuilder} to
   * support custom attributes.
   * <p>
   * If the base path does not exist, it will be created.
   * <p>
   * If the base path exists and if the N5 version of the container is compatible with this
   * implementation, the N5 version of this container will be set to the current N5 version of this
   * implementation.
   *
   * @param keyValueAccess
   * @param basePath        n5 base path
   * @param gsonBuilder
   * @param cacheAttributes cache attributes Setting this to true avoids frequent reading and
   *                        parsing of JSON encoded attributes, this is most interesting for high
   *                        latency file systems. Changes of attributes by an independent writer
   *                        will not be tracked.
   * @throws N5Exception if the base path cannot be written to or cannot be created.
   */
  public Zarr3KeyValueWriter(
      final KeyValueAccess keyValueAccess,
      final String basePath,
      final GsonBuilder gsonBuilder,
      final boolean mapN5DatasetAttributes,
      final ChunkKeyEncoding chunkKeyEncoding,
      final boolean cacheAttributes)
      throws N5Exception {

    super(
        false,
        keyValueAccess,
        basePath,
        gsonBuilder,
        mapN5DatasetAttributes,
        cacheAttributes);
    this.chunkKeyEncoding = chunkKeyEncoding;
    Version version = null;
    if (exists("/")) {
      version = getVersion();
      if (!Zarr3KeyValueReader.VERSION.isCompatible(version)) {
        throw new N5IOException(
            "Incompatible version " + version + " (this is " + Zarr3KeyValueReader.VERSION + ").");
      }
    }

    if (version == null || version == VERSION_ZERO) {
      createGroup("/");
      setVersion("/");
    }
  }

  /**
   * Writes a {@link DataBlock} into an {@link OutputStream}.
   *
   * @param out               the output stream
   * @param datasetAttributes dataset attributes
   * @param dataBlock         the data block
   * @throws IOException the exception
   */
  public static <T> void writeBlock(
      final OutputStream out,
      final Zarr3DatasetAttributes datasetAttributes,
      final DataBlock<T> dataBlock) throws IOException {

    final int[] blockSize = datasetAttributes.getBlockSize();
    final DataType dataType = datasetAttributes.getDType();
    final BlockWriter writer = datasetAttributes.getCompression().getWriter();

    if (!Arrays.equals(blockSize, dataBlock.getSize())) {

      final byte[] padCropped = padCrop(
          dataBlock.toByteBuffer().array(),
          dataBlock.getSize(),
          blockSize,
          dataType.getNBytes(),
          datasetAttributes.getFillBytes());

      final DataBlock<byte[]> padCroppedDataBlock = new ByteArrayDataBlock(
          blockSize,
          dataBlock.getGridPosition(),
          padCropped);

      writer.write(padCroppedDataBlock, out);

    } else {

      writer.write(dataBlock, out);
    }
  }

  public static byte[] padCrop(
      final byte[] src,
      final int[] srcBlockSize,
      final int[] dstBlockSize,
      final int nBytes,
      final byte[] fill_value) {

    assert srcBlockSize.length == dstBlockSize.length : "Dimensions do not match.";

    final int n = srcBlockSize.length;

    if (nBytes != 0) {
      final int[] srcStrides = new int[n];
      final int[] dstStrides = new int[n];
      final int[] srcSkip = new int[n];
      final int[] dstSkip = new int[n];
      srcStrides[0] = dstStrides[0] = nBytes;
      for (int d = 1; d < n; ++d) {
        srcStrides[d] = srcBlockSize[d] * srcBlockSize[d - 1];
        dstStrides[d] = dstBlockSize[d] * dstBlockSize[d - 1];
      }
      for (int d = 0; d < n; ++d) {
        srcSkip[d] = Math.max(1, dstBlockSize[d] - srcBlockSize[d]);
        dstSkip[d] = Math.max(1, srcBlockSize[d] - dstBlockSize[d]);
      }

      /* this is getting hairy, ImgLib2 alternative */
      /* byte images with 0-dimension d[0] * nBytes */
      final long[] srcIntervalDimensions = new long[n];
      final long[] dstIntervalDimensions = new long[n];
      srcIntervalDimensions[0] = srcBlockSize[0] * nBytes;
      dstIntervalDimensions[0] = dstBlockSize[0] * nBytes;
      for (int d = 1; d < n; ++d) {
        srcIntervalDimensions[d] = srcBlockSize[d];
        dstIntervalDimensions[d] = dstBlockSize[d];
      }

      final byte[] dst = new byte[(int) Intervals.numElements(dstIntervalDimensions)];
      /* fill dst */
      for (int i = 0, j = 0; i < n; ++i) {
        dst[i] = fill_value[j];
        if (++j == fill_value.length) {
          j = 0;
        }
      }
      final ArrayImg<ByteType, ByteArray> srcImg = ArrayImgs.bytes(src, srcIntervalDimensions);
      final ArrayImg<ByteType, ByteArray> dstImg = ArrayImgs.bytes(dst, dstIntervalDimensions);

      final FinalInterval intersection = Intervals.intersect(srcImg, dstImg);
      final Cursor<ByteType> srcCursor = Views.interval(srcImg, intersection).cursor();
      final Cursor<ByteType> dstCursor = Views.interval(dstImg, intersection).cursor();
      while (srcCursor.hasNext()) {
        dstCursor.next().set(srcCursor.next());
      }

      return dst;
    } else {
      return null;
    }
  }

  protected static void redirectDatasetAttribute(
      final JsonObject src,
      final String srcKey,
      final JsonObject dest,
      final String destKey) {

    if (src.has(srcKey)) {
      final JsonElement e = src.get(srcKey);
      dest.add(destKey, e);
      src.remove(srcKey);
    }
  }

  protected static void redirectChunkGrid(final JsonObject src, final JsonObject dest) {
    JsonObject chunkGridConfigurationObject = new JsonObject();
    chunkGridConfigurationObject.add("chunk_shape", src.get(DatasetAttributes.BLOCK_SIZE_KEY));
    JsonObject chunkGridObject = new JsonObject();
    chunkGridObject.add("name", new JsonPrimitive("regular"));
    chunkGridObject.add("configuration", chunkGridConfigurationObject);
    dest.add(Zarr3ArrayAttributes.chunkGridKey, chunkGridObject);
    src.remove(DatasetAttributes.BLOCK_SIZE_KEY);
  }

  protected static void redirectDataType(final JsonObject src, final JsonObject dest) {

    if (src.has(DatasetAttributes.DATA_TYPE_KEY)) {
      final JsonElement e = src.get(DatasetAttributes.DATA_TYPE_KEY);
      dest.addProperty(Zarr3ArrayAttributes.dataTypeKey,
          new DataType(org.janelia.saalfeldlab.n5.DataType.fromString(e.getAsString())).toString());
      src.remove(DatasetAttributes.DATA_TYPE_KEY);
    }
  }

  protected static void redirectCompression(final JsonObject src, final Gson gson,
      final JsonObject dest) {

    // TODO: Codecs
    if (src.has(DatasetAttributes.COMPRESSION_KEY)) {
      final Compression c = gson.fromJson(src.get(DatasetAttributes.COMPRESSION_KEY),
          Compression.class);
      if (c.getClass() == RawCompression.class) {
        dest.add(Zarr3ArrayAttributes.codecsKey, JsonNull.INSTANCE);
      } else {
        dest.add(Zarr3ArrayAttributes.codecsKey,
            gson.toJsonTree(ZarrCodec.fromCompression(c)));
      }

      src.remove(DatasetAttributes.COMPRESSION_KEY);
    }
  }

  protected static JsonObject build(final JsonObject obj, final Gson gson) {

    return build(obj, gson, true);
  }

  /**
   * Creates a {@link JsonObject} object from a {@link JsonObject} containing attributes.
   * <p>
   * Used when re-directing attributes to the appropriate zarr metadata files according to attribute
   * keys.
   *
   * @param obj             the json attributes object
   * @param gson            the json
   * @param mapN5Attributes if true, map n5 attribute keys to corresponding zarr attribute keys
   * @return
   */
  protected static JsonObject build(final JsonObject obj, final Gson gson,
      final boolean mapN5Attributes) {

    // first map n5 attributes
    if (mapN5Attributes) {
      redirectDatasetAttribute(obj, DatasetAttributes.DIMENSIONS_KEY, obj,
          Zarr3ArrayAttributes.shapeKey);
      redirectChunkGrid(obj, obj);
      redirectDataType(obj, obj);
      redirectCompression(obj, gson, obj);
    }

    // put relevant attributes in appropriate JsonElements
    final JsonObject out = new JsonObject();
    // make the array json
    if (hasRequiredArrayKeys(obj)) {
      out.add(NODE_TYPE_KEY, new JsonPrimitive(NODE_TYPE_ARRAY));
      move(obj, out, Zarr3ArrayAttributes.allKeys);
      reverseAttrsWhenCOrder(out);
    } else if (obj.has(ZARR_FORMAT_KEY)) {
      out.add(NODE_TYPE_KEY, new JsonPrimitive(NODE_TYPE_GROUP));
      move(obj, out, ZARR_FORMAT_KEY);
    }

    // whatever remains goes into user attributes
    out.add("attributes", obj);

    return out;
  }

  protected static boolean hasRequiredArrayKeys(final JsonObject obj) {
    return Arrays.stream(Zarr3ArrayAttributes.requiredKeys).allMatch(obj::has);
  }

  protected static void move(final JsonObject src, final JsonObject dst,
      final String... keys) {

    for (final String key : keys) {
      if (src.has(key)) {
        dst.add(key, src.get(key));
        src.remove(key);
      }
    }
  }

  static void reorder(final long[] array) {

    long a;
    final int max = array.length - 1;
    for (int i = (max - 1) / 2; i >= 0; --i) {
      final int j = max - i;
      a = array[i];
      array[i] = array[j];
      array[j] = a;
    }
  }

  static void reorder(final int[] array) {

    int a;
    final int max = array.length - 1;
    for (int i = (max - 1) / 2; i >= 0; --i) {
      final int j = max - i;
      a = array[i];
      array[i] = array[j];
      array[j] = a;
    }
  }

  static void reorder(final JsonArray array) {

    JsonElement a;
    final int max = array.size() - 1;
    for (int i = (max - 1) / 2; i >= 0; --i) {
      final int j = max - i;
      a = array.get(i);
      array.set(i, array.get(j));
      array.set(j, a);
    }
  }

  @Override
  public void setVersion(final String path) throws N5Exception {
    final String normalPath = N5URI.normalizeGroupPath(path);
    if (exists(path)) {
      final JsonObject zarrJson = getZarrJson(path).getAsJsonObject();
      zarrJson.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5Zarr3Reader.VERSION.getMajor()));
      if (!zarrJson.has(NODE_TYPE_KEY)) {
        zarrJson.add(NODE_TYPE_KEY, new JsonPrimitive(NODE_TYPE_GROUP));
      }
      writeZarrJson(normalPath, zarrJson); // updates cache
    }
  }

  @Override
  public void createGroup(final String path) throws N5Exception {

    final String normalPath = N5URI.normalizeGroupPath(path);
    // avoid hitting the backend if this path is already a group according to the cache
    // else if exists is true (then a dataset is present) so throw an exception to avoid
    // overwriting / invalidating existing data
    if (cacheMeta()) {
      if (getCache().isGroup(normalPath, ZARR_JSON_FILE)) {
        return;
      } else if (getCache().isDataset(normalPath, ZARR_JSON_FILE)) {
        throw new N5Exception("Can't make a group on existing dataset.");
      }
    }

    // Overridden to change the cache key, though it may not be necessary
    // since the contents is null
    try {
      keyValueAccess.createDirectories(absoluteGroupPath(normalPath));
    } catch (final Throwable e) {
      throw new N5IOException("Failed to create group " + path, e);
    }

    final JsonObject zarrGroupObject = new JsonObject();
    zarrGroupObject.add(ZARR_FORMAT_KEY, new JsonPrimitive(N5Zarr3Reader.VERSION.getMajor()));
    zarrGroupObject.add(NODE_TYPE_KEY, new JsonPrimitive(NODE_TYPE_GROUP));

    String[] pathParts = getKeyValueAccess().components(normalPath);
    String parent = N5URI.normalizeGroupPath("/");
    if (pathParts.length == 0) {
      pathParts = new String[]{""};
    }

    for (final String child : pathParts) {

      final String childPath = parent.isEmpty() ? child : parent + "/" + child;
      if (cacheMeta()) {
        // TODO: Maybe already handled by writeZarrJson?
        getCache().initializeNonemptyCache(childPath, ZARR_JSON_FILE);
        getCache().updateCacheInfo(childPath, ZARR_JSON_FILE, zarrGroupObject);

        // only add if the parent exists and has children cached already
        if (parent != null && !child.isEmpty()) {
          getCache().addChildIfPresent(parent, child);
        }
      }

      writeZarrJson(childPath, zarrGroupObject);
      parent = childPath;
    }
  }

  @Override
  public void createDataset(
      final String path,
      final DatasetAttributes datasetAttributes) throws N5Exception {

    final String normalPath = N5URI.normalizeGroupPath(path);
    if (cacheMeta()) {
      if (getCache().isDataset(normalPath, ZARR_JSON_FILE)) {
        return;
      } else if (getCache().isGroup(normalPath, ZARR_JSON_FILE)) {
        // TODO tests currently require that we can make a dataset on a group
//				throw new N5Exception("Can't make a group on existing path.");
      }
    }

    // Overriding because CachedGsonKeyValueWriter calls createGroup.
    // Not correct for zarr, since groups and datasets are mutually
    // exclusive
    final String absPath = absoluteGroupPath(normalPath);
    try {
      keyValueAccess.createDirectories(absPath);
    } catch (final Throwable e) {
      throw new N5IOException("Failed to create directories " + absPath, e);
    }

    // create parent group
    final String[] pathParts = keyValueAccess.components(normalPath);
    final String parent = Arrays.stream(pathParts).limit(pathParts.length - 1)
        .collect(Collectors.joining("/"));
    createGroup(parent);

    // These three lines are preferable to setDatasetAttributes because they
    // are more efficient wrt caching
    final Zarr3ArrayAttributes arrayAttributes = createZarr3ArrayAttributes(datasetAttributes);
    final JsonElement attributes = gson.toJsonTree(arrayAttributes.asMap());
    writeJsonResource(normalPath, ZARR_JSON_FILE, attributes);

    if (cacheMeta()) {
      // cache dataset and add as child to parent if necessary
      getCache().initializeNonemptyCache(normalPath, ZARR_JSON_FILE);
      getCache().updateCacheInfo(normalPath, ZARR_JSON_FILE, attributes);
      getCache().addChildIfPresent(parent, pathParts[pathParts.length - 1]);
    }
  }

  @Override
  public void setAttributes(
      final String path,
      final Map<String, ?> attributes) throws N5Exception {

    final String normalPath = N5URI.normalizeGroupPath(path);
    if (!exists(normalPath)) {
      throw new N5IOException("" + normalPath + " is not a group or dataset.");
    }

    // cache here or in writeAttributes?
    // I think in writeAttributes is better - let it delegate to
    // writeZArray, writeZAttrs, writeZGroup
    final JsonElement existingAttributes = getAttributesUnmapped(normalPath); // uses cache
    JsonElement newAttributes = existingAttributes != null && existingAttributes.isJsonObject()
        ? existingAttributes.getAsJsonObject()
        : new JsonObject();
    newAttributes = GsonUtils.insertAttributes(newAttributes, attributes, gson);

    if (newAttributes.isJsonObject()) {
      writeZarrJson(normalPath,
          build(newAttributes.getAsJsonObject(), getGson()));// handles caching
    } else {
      throw new N5Exception("Attributes are not an object");
    }
  }

  @Override
  public boolean removeAttribute(final String pathName, final String key) throws N5Exception {

    final String normalPath = N5URI.normalizeGroupPath(pathName);
    final String normalKey = N5URI.normalizeAttributePath(key);
    if (!keyValueAccess.exists(keyValueAccess.compose(uri, normalPath, ZARR_JSON_FILE))) {
      return false;
    }

    final JsonElement attributes = getAttributesUnmapped(normalPath); // uses cache
    final JsonElement userAttributes = attributes.getAsJsonObject().get("attributes");

    if (key.equals("/")) {
      attributes.getAsJsonObject().add("attributes", new JsonObject());
      writeJsonResource(normalPath, ZARR_JSON_FILE, attributes);
      if (cacheMeta()) {
        cache.updateCacheInfo(normalPath, ZARR_JSON_FILE, attributes);
      }

      return true;
    } else if (GsonUtils.removeAttribute(userAttributes, normalKey) != null) {

      if (cacheMeta()) {
        cache.updateCacheInfo(normalPath, ZARR_JSON_FILE, attributes);
      }

      writeJsonResource(normalPath, ZARR_JSON_FILE, attributes);
      return true;
    }
    return false;
  }

  @Override
  public <T> T removeAttribute(final String pathName, final String key, final Class<T> cls)
      throws N5Exception {

    final String normalPath = N5URI.normalizeGroupPath(pathName);
    final String normalKey = N5URI.normalizeAttributePath(key);

    final JsonElement attributes = getAttributesUnmapped(normalPath); // uses cache
    final JsonElement userAttributes = attributes.getAsJsonObject().get("attributes");
    final T obj;
    try {
      obj = GsonUtils.removeAttribute(userAttributes, normalKey, cls, gson);
    } catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
    if (obj != null) {
      if (cacheMeta()) {
        cache.updateCacheInfo(normalPath, ZARR_JSON_FILE, attributes);
      }

      writeJsonResource(normalPath, ZARR_JSON_FILE, attributes);
    }
    return obj;
  }

  @Override
  public void setDatasetAttributes(
      final String pathName,
      final DatasetAttributes datasetAttributes) throws N5Exception {

    setZarr3ArrayAttributes(pathName, createZarr3ArrayAttributes(datasetAttributes));
  }

  protected Zarr3ArrayAttributes createZarr3ArrayAttributes(
      final DatasetAttributes datasetAttributes) {

    final long[] shape = datasetAttributes.getDimensions().clone();
    reorder(shape);
    final RegularChunkGrid chunkGrid = new RegularChunkGrid(
        datasetAttributes.getBlockSize().clone());
    reorder(chunkGrid.chunkShape);
    final DataType dataType = new DataType(datasetAttributes.getDataType());

    final Zarr3ArrayAttributes zArrayAttributes = new Zarr3ArrayAttributes(
        shape,
        dataType,
        chunkGrid,
        chunkKeyEncoding,
        (Zarr3CodecPipeline) datasetAttributes.getCompression(),
        dataType.defaultFillValue(), new JsonObject());

    return zArrayAttributes;
  }

  /**
   * Write the contents of the attributes argument to the .zarray file at the given pathName.
   * Overwrites and existing .zarray.
   *
   * @param pathName   the group / dataset path
   * @param attributes ZArray attributes
   * @throws N5Exception the exception
   */
  public void setZarr3ArrayAttributes(final String pathName, final Zarr3ArrayAttributes attributes)
      throws N5Exception {

    writeZarrJson(pathName, gson.toJsonTree(attributes.asMap()));
  }

  protected void deleteJsonResource(final String normalPath, final String jsonName)
      throws N5Exception {
    final String absolutePath = keyValueAccess.compose(uri, normalPath, jsonName);
    try {
      keyValueAccess.delete(absolutePath);
    } catch (final Throwable e1) {
      throw new N5IOException("Failed to delete " + absolutePath, e1);
    }
  }

  protected void writeJsonResource(
      final String normalPath,
      final String jsonName,
      final JsonElement attributes) throws N5Exception {

    if (attributes == null) {
      return;
    }

    final String absolutePath = keyValueAccess.compose(uri, normalPath, jsonName);
    try (final LockedChannel lock = keyValueAccess.lockForWriting(absolutePath)) {
      GsonUtils.writeAttributes(lock.newWriter(), attributes, gson);
    } catch (final Throwable e) {
      throw new N5IOException("Failed to write " + absolutePath, e);
    }
  }

  protected void writeZarrJson(
      final String normalGroupPath,
      final JsonElement zarrJsonObject) throws N5Exception {

    if (zarrJsonObject == null) {
      return;
    }

    writeJsonResource(normalGroupPath, ZARR_JSON_FILE, zarrJsonObject);
    if (cacheMeta()) {
      cache.updateCacheInfo(normalGroupPath, ZARR_JSON_FILE, zarrJsonObject);
    }
  }

  @Override
  public <T> void writeBlock(
      final String pathName,
      final DatasetAttributes datasetAttributes,
      final DataBlock<T> dataBlock) throws N5Exception {

    final Zarr3DatasetAttributes zarrDatasetAttributes;
    if (datasetAttributes instanceof Zarr3DatasetAttributes) {
      zarrDatasetAttributes = (Zarr3DatasetAttributes) datasetAttributes;
    } else {
      zarrDatasetAttributes = getDatasetAttributes(pathName); // TODO is
    }
    // this
    // correct?

    final String normalPath = N5URI.normalizeGroupPath(pathName);
    final String path = keyValueAccess
        .compose(
            uri,
            normalPath,
            zarrDatasetAttributes.getChunkKeyEncoding()
                .encodeChunkKey(dataBlock.getGridPosition()));

    final String[] components = keyValueAccess.components(path);
    final String parent = keyValueAccess
        .compose(Arrays.stream(components).limit(components.length - 1).toArray(String[]::new));
    try {
      keyValueAccess.createDirectories(parent);
      try (final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path)) {

        writeBlock(
            lockedChannel.newOutputStream(),
            zarrDatasetAttributes,
            dataBlock);
      }
    } catch (final Throwable e) {
      throw new N5IOException(
          "Failed to write block " + Arrays.toString(dataBlock.getGridPosition()) + " into dataset "
              + path,
          e);
    }
  }

  @Override
  public boolean deleteBlock(
      final String path,
      final long... gridPosition) throws N5Exception {

    final String normPath = N5URI.normalizeGroupPath(path);
    final Zarr3DatasetAttributes zarrDatasetAttributes = getDatasetAttributes(normPath);
    final String absolutePath = keyValueAccess
        .compose(
            uri,
            normPath,
            zarrDatasetAttributes.getChunkKeyEncoding()
                .encodeChunkKey(gridPosition));

    try {
      if (keyValueAccess.exists(absolutePath)) {
        keyValueAccess.delete(absolutePath);
      }
    } catch (final Throwable e) {
      throw new N5IOException(
          "Failed to delete block " + Arrays.toString(gridPosition) + " from dataset " + path,
          e);
    }

    /* an IOException should have occurred if anything had failed midway */
    return true;
  }
}
