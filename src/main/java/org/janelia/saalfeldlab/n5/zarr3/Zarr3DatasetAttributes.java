/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2019 - 2022 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Objects;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.zarr3.ChunkGrid.RegularChunkGrid;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class Zarr3DatasetAttributes extends DatasetAttributes {

  public static final String shapeKey = "shape";
  public static final String dataTypeKey = "data_type";
  public static final String chunkGridKey = "chunk_grid";
  public static final String chunkKeyEncodingKey = "chunk_key_encoding";
  public static final String codecsKey = "codecs";
  public static final String fillValueKey = "fill_value";
  public static final String attributesKey = "attributes";

  public static final String[] allKeys = new String[]{Zarr3KeyValueReader.ZARR_FORMAT_KEY,
      Zarr3KeyValueReader.NODE_TYPE_KEY, shapeKey, dataTypeKey, chunkGridKey, chunkKeyEncodingKey,
      codecsKey, fillValueKey, attributesKey
  };
  public static JsonAdapter jsonAdapter = new JsonAdapter();
  private final ChunkKeyEncoding chunkKeyEncoding;
  private final Object fillValue;

  private final JsonObject attributes;

  private final transient byte[] fillBytes;

  public Zarr3DatasetAttributes(
      final long[] shape,
      final DataType dataType,
      final ChunkGrid chunkGrid,
      final ChunkKeyEncoding chunkKeyEncoding,
      final Zarr3CodecPipeline codecs,
      final Object fillValue,
      final JsonObject attributes
  ) {
    super(shape, ((RegularChunkGrid)chunkGrid).chunkShape, dataType.getDataType(), codecs);
    this.fillValue = fillValue;
    this.chunkKeyEncoding = chunkKeyEncoding;
    this.attributes = attributes;
    this.fillBytes = dataType.createFillBytes(fillValue);
  }

  public long[] getShape() {

    return getDimensions();
  }

  public ChunkKeyEncoding getChunkKeyEncoding() {
    return chunkKeyEncoding;
  }

  public ChunkGrid getChunkGrid() {
    return new RegularChunkGrid(super.getBlockSize());
  }

  public Zarr3CodecPipeline getCodecs() {

    return (Zarr3CodecPipeline)getCompression();
  }

  public DataType getDType() {

    return new DataType(super.getDataType());
  }

  public int getZarrFormat() {

    return Zarr3KeyValueReader.VERSION.getMajor();
  }

  public Object getFillValue() {

    return fillValue;
  }

  public byte[] getFillBytes() {

    return fillBytes;
  }

  public JsonObject getAttributes() {

    return attributes;
  }

  public HashMap<String, Object> asMap() {

    final HashMap<String, Object> map = new HashMap<>();

    map.put(Zarr3KeyValueReader.ZARR_FORMAT_KEY, Zarr3KeyValueReader.VERSION.getMajor());
    map.put(Zarr3KeyValueReader.NODE_TYPE_KEY, Zarr3KeyValueReader.NODE_TYPE_ARRAY);
    map.put(shapeKey, getShape());
    map.put(dataTypeKey, getDType().toString());
    map.put(chunkGridKey, getChunkGrid().asMap());
    map.put(chunkKeyEncodingKey, chunkKeyEncoding.asMap());
    map.put(fillValueKey, fillValue);
    map.put(codecsKey, getCodecs().getCodecs());
    map.put(attributesKey, attributes);

    return map;
  }

  public JsonObject asN5Attributes(Gson gson) {
    final JsonObject attrs = gson.toJsonTree(asMap()).getAsJsonObject();

    attrs.add(DatasetAttributes.DIMENSIONS_KEY, Utils.toJsonArray(getShape()));
    attrs.add(DatasetAttributes.BLOCK_SIZE_KEY,
        Utils.toJsonArray(((RegularChunkGrid) getChunkGrid()).chunkShape));
    attrs.addProperty(DatasetAttributes.DATA_TYPE_KEY, getDType().getDataType().toString());

    // TODO: Codecs
    final JsonElement e = attrs.get(Zarr3DatasetAttributes.codecsKey);
    if (e == JsonNull.INSTANCE) {
      attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(new RawCompression()));
    } else {
      attrs.add(DatasetAttributes.COMPRESSION_KEY, gson.toJsonTree(getCodecs()));
    }

    return attrs;
  }

  public <T> void setAttribute(final String keyPath, final T value) {
  }

  public <T> T getAttribute(final String keyPath, final Class<T> clazz) {
    return null;
  }

  public <T> T removeAttribute(final String keyPath, final Class<T> clazz) {
    return null;
  }

  public static class JsonAdapter implements JsonDeserializer<Zarr3DatasetAttributes> {

    @Override
    public Zarr3DatasetAttributes deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {

      final JsonObject obj = json.getAsJsonObject();
      try {
        if (!Objects.equals(obj.get(Zarr3KeyValueReader.NODE_TYPE_KEY).getAsString(),
            Zarr3KeyValueReader.NODE_TYPE_ARRAY)) {
          return null;
        }

        final String typestr = obj.get(dataTypeKey).getAsString();
        final DataType dataType = new DataType(typestr);

        final JsonPrimitive fillValueJson = obj.get(fillValueKey).getAsJsonPrimitive();
        Object fillValue;
        if (fillValueJson.isBoolean()) {
          fillValue = fillValueJson.getAsBoolean();
        } else if (fillValueJson.isString()) {
          fillValue = fillValueJson.getAsString();
        } else if (fillValueJson.isNumber()) {
          fillValue = fillValueJson.getAsNumber();
        } else {
          throw new RuntimeException("Unreachable");
        }

        return new Zarr3DatasetAttributes(
            context.deserialize(obj.get(shapeKey), long[].class),
            dataType,
            context.deserialize(obj.get(chunkGridKey), ChunkGrid.class),
            context.deserialize(obj.get(chunkKeyEncodingKey), ChunkKeyEncoding.class),
            context.deserialize(obj.get(codecsKey), Zarr3CodecPipeline.class), // fix
            fillValue,
            obj.get(attributesKey).getAsJsonObject()
        );
      } catch (Exception e) {
        return null;
      }
    }

  }
}
