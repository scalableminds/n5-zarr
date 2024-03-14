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
import java.util.Map;
import java.util.Objects;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.zarr3.ChunkGrid.RegularChunkGrid;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class Zarr3DatasetAttributes extends DatasetAttributes implements
    Zarr3Attributes {

  public static final String shapeKey = "shape";
  public static final String dataTypeKey = "data_type";
  public static final String chunkGridKey = "chunk_grid";
  public static final String chunkKeyEncodingKey = "chunk_key_encoding";
  public static final String codecsKey = "codecs";
  public static final String fillValueKey = "fill_value";
  public static final String attributesKey = "attributes";

  public static final String[] allKeys = new String[]{ZARR_FORMAT_KEY,
      NODE_TYPE_KEY, shapeKey, dataTypeKey, chunkGridKey, chunkKeyEncodingKey,
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
    super(shape, ((RegularChunkGrid) chunkGrid).chunkShape, dataType.getDataType(), codecs);
    this.fillValue = fillValue;
    this.chunkKeyEncoding = chunkKeyEncoding;
    this.attributes = attributes;
    this.fillBytes = dataType.createFillBytes(fillValue);
  }

  public static Zarr3DatasetAttributes fromDatasetAttributes(DatasetAttributes datasetAttributes,
      ChunkKeyEncoding chunkKeyEncoding) {
    final long[] shape = datasetAttributes.getDimensions().clone();
    final RegularChunkGrid chunkGrid = new RegularChunkGrid(
        datasetAttributes.getBlockSize().clone());
    final DataType dataType = new DataType(datasetAttributes.getDataType());

    Zarr3DatasetAttributes zarr3DatasetAttributes = new Zarr3DatasetAttributes(
        shape,
        dataType,
        chunkGrid,
        chunkKeyEncoding,
        Zarr3CodecPipeline.guessCompression(datasetAttributes.getCompression()),
        dataType.defaultFillValue(), new JsonObject());
    return zarr3DatasetAttributes;
  }

  public static Zarr3DatasetAttributes fromJson(JsonElement jsonElement, Gson gson) {
    return gson.fromJson(jsonElement, Zarr3DatasetAttributes.class);
  }

  public static Zarr3DatasetAttributes fromN5Attributes(JsonElement jsonElement, Gson gson) {
    JsonObject attrs = jsonElement.getAsJsonObject().deepCopy();
    JsonBuilder newAttrs = JsonBuilder.object();

    newAttrs.addProperty(shapeKey, attrs.get(DIMENSIONS_KEY));
    attrs.remove(DIMENSIONS_KEY);

    newAttrs.addObject(chunkGridKey, c -> c
        .addProperty("name", "regular")
        .addObject("configuration",
            c1 -> c1.addArray("chunk_shape", attrs.get(BLOCK_SIZE_KEY).getAsJsonArray())));
    attrs.remove(BLOCK_SIZE_KEY);

    newAttrs.addProperty(dataTypeKey, new DataType(org.janelia.saalfeldlab.n5.DataType.fromString(
        attrs.get(DATA_TYPE_KEY).getAsString())).toString());
    attrs.remove(DATA_TYPE_KEY);

    // Codecs
    JsonElement compression = attrs.get(COMPRESSION_KEY);
    if (compression == null || compression == JsonNull.INSTANCE) {
      newAttrs.addArray(codecsKey, a -> a.addObject(c -> c
          .addProperty("name", "bytes")
          .addObject("configuration", c1 -> c1.addProperty("endian", "little"))));
    } else {
      newAttrs.addProperty(codecsKey, compression);
    }
    attrs.remove(COMPRESSION_KEY);

    newAttrs.addProperty(ZARR_FORMAT_KEY, ZARR_3.getMajor());
    attrs.remove(ZARR_FORMAT_KEY);

    newAttrs.addProperty(NODE_TYPE_KEY, NODE_TYPE_ARRAY);
    attrs.remove(NODE_TYPE_KEY);

    newAttrs.addProperty(chunkKeyEncodingKey, attrs.get(chunkKeyEncodingKey));
    attrs.remove(chunkKeyEncodingKey);

    newAttrs.addProperty(fillValueKey, attrs.get(fillValueKey));
    attrs.remove(fillValueKey);

    newAttrs.addObject("attributes", attrs);

    return gson.fromJson(newAttrs.build(), Zarr3DatasetAttributes.class);
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

    return (Zarr3CodecPipeline) getCompression();
  }

  public DataType getDType() {

    return new DataType(super.getDataType());
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

  @Override
  public Zarr3Attributes setAttributes(final Map<String, ?> attributes, Gson gson) {
    JsonElement newAttributes = GsonUtils.insertAttributes(asN5Attributes(gson), attributes, gson);
    return fromN5Attributes(newAttributes, gson);
  }

  @Override
  public Zarr3Attributes removeAttribute(final String keyPath, Gson gson) {
    final String normalKey = N5URI.normalizeAttributePath(keyPath);
    if (keyPath.equals("/")) {
      return new Zarr3DatasetAttributes(getShape(), getDType(), getChunkGrid(),
          getChunkKeyEncoding(), getCodecs(), getFillValue(), new JsonObject());
    }
    JsonElement newAttributes = GsonUtils.removeAttribute(asN5Attributes(gson), normalKey);
    return fromN5Attributes(newAttributes, gson);
  }

  public HashMap<String, Object> asMap() {

    final HashMap<String, Object> map = new HashMap<>();

    map.put(ZARR_FORMAT_KEY, ZARR_3.getMajor());
    map.put(NODE_TYPE_KEY, NODE_TYPE_ARRAY);
    map.put(shapeKey, getShape());
    map.put(dataTypeKey, getDType().toString());
    map.put(chunkGridKey, getChunkGrid().asMap());
    map.put(chunkKeyEncodingKey, chunkKeyEncoding.asMap());
    map.put(fillValueKey, fillValue);
    map.put(codecsKey, getCodecs().getCodecs());
    map.put(attributesKey, attributes);

    return map;
  }

  @Override
  public JsonObject asN5Attributes(Gson gson) {
    JsonBuilder newAttrs = JsonBuilder.object();

    newAttrs.addProperty(ZARR_FORMAT_KEY, ZARR_3.getMajor());
    newAttrs.addProperty(NODE_TYPE_KEY, NODE_TYPE_ARRAY);
    newAttrs.addArray(DIMENSIONS_KEY, getShape());
    newAttrs.addArray(BLOCK_SIZE_KEY, ((RegularChunkGrid) getChunkGrid()).chunkShape);
    newAttrs.addProperty(DATA_TYPE_KEY, getDType().getDataType().toString());
    newAttrs.addProperty(chunkKeyEncodingKey, gson.toJsonTree(chunkKeyEncoding.asMap()));
    newAttrs.addProperty(fillValueKey, gson.toJsonTree(fillValue));
    newAttrs.addProperty(COMPRESSION_KEY, getCodecs().getCodecs());

    JsonObject userAttributes = attributes.getAsJsonObject();
    for (Map.Entry<String, JsonElement> entry : userAttributes.entrySet()) {
      newAttrs.addProperty(entry.getKey(), entry.getValue());
    }
    return newAttrs.build();
  }

  public static class JsonAdapter implements JsonDeserializer<Zarr3DatasetAttributes> {

    @Override
    public Zarr3DatasetAttributes deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {

      final JsonObject obj = json.getAsJsonObject();
      if (!obj.has(ZARR_FORMAT_KEY)) {
        throw new JsonParseException(String.format("Key `%s` is missing.", ZARR_FORMAT_KEY));
      }
      if (obj.get(ZARR_FORMAT_KEY).getAsInt() != ZARR_3.getMajor()) {
        throw new JsonParseException(String.format("Key `%s` is invalid", ZARR_FORMAT_KEY));
      }

      if (!obj.has(NODE_TYPE_KEY)) {
        throw new JsonParseException(String.format("Key `%s` is missing.", NODE_TYPE_KEY));
      }
      if (!Objects.equals(obj.get(NODE_TYPE_KEY).getAsString(), NODE_TYPE_ARRAY)) {
        throw new JsonParseException(String.format("Key `%s` is invalid.", NODE_TYPE_KEY));
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

      JsonObject attributes =
          obj.has(attributesKey) ? obj.get(attributesKey).getAsJsonObject() : new JsonObject();

      return new Zarr3DatasetAttributes(
          context.deserialize(obj.get(shapeKey), long[].class),
          dataType,
          context.deserialize(obj.get(chunkGridKey), ChunkGrid.class),
          context.deserialize(obj.get(chunkKeyEncodingKey), ChunkKeyEncoding.class),
          context.deserialize(obj.get(codecsKey), Zarr3CodecPipeline.class), // fix
          fillValue,
          attributes
      );
    }

  }
}
