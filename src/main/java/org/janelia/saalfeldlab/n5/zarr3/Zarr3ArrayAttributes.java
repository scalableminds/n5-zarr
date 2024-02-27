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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import java.lang.reflect.Type;
import java.util.HashMap;
import org.janelia.saalfeldlab.n5.zarr3.ChunkGrid.RegularChunkGrid;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class Zarr3ArrayAttributes {

  public static final String shapeKey = "shape";
  public static final String dataTypeKey = "data_type";
  public static final String chunkGridKey = "chunk_grid";
  public static final String chunkKeyEncodingKey = "chunk_key_encoding";
  public static final String codecsKey = "codecs";
  public static final String fillValueKey = "fill_value";
  public static final String attributesKey = "attributes";

  public static final String[] requiredKeys = new String[]{
      Zarr3KeyValueReader.ZARR_FORMAT_KEY, Zarr3KeyValueReader.NODE_TYPE_KEY, chunkGridKey,
      dataTypeKey, codecsKey, fillValueKey,
      chunkKeyEncodingKey
  };

  public static final String[] allKeys = new String[]{Zarr3KeyValueReader.ZARR_FORMAT_KEY,
      Zarr3KeyValueReader.NODE_TYPE_KEY, chunkGridKey,
      dataTypeKey, codecsKey,
      fillValueKey, chunkKeyEncodingKey
  };
  public static JsonAdapter jsonAdapter = new JsonAdapter();
  private final long[] shape;
  private final DataType dataType;
  private final ChunkGrid chunkGrid;
  private final ChunkKeyEncoding chunkKeyEncoding;
  private final Zarr3CodecPipeline codecs;
  private final Object fillValue;

  private final JsonObject attributes;

  public Zarr3ArrayAttributes(
      final long[] shape,
      final DataType dataType,
      final ChunkGrid chunkGrid,
      final ChunkKeyEncoding chunkKeyEncoding,
      final Zarr3CodecPipeline codecs,
      final Object fillValue,
      final JsonObject attributes
  ) {
    this.shape = shape;
    this.chunkGrid = chunkGrid;
    this.dataType = dataType;
    this.codecs = codecs;
    this.fillValue = fillValue;
    this.chunkKeyEncoding = chunkKeyEncoding;
    this.attributes = attributes;
  }

  public Zarr3DatasetAttributes getDatasetAttributes() {

    final long[] dimensions = shape.clone();
    final int[] blockSize = ((RegularChunkGrid) chunkGrid).chunkShape.clone();

    //  if (isRowMajor) {
    //    ZarrKeyValueWriter.reorder(dimensions);
    //    ZarrKeyValueWriter.reorder(blockSize);
    //  }

    return new Zarr3DatasetAttributes(
        dimensions,
        blockSize,
        dataType,
        codecs,
        fillValue,
        chunkKeyEncoding);
  }

  public long[] getShape() {

    return shape;
  }

  public int getNumDimensions() {

    return shape.length;
  }

  public int[] getChunks() {

    return ((RegularChunkGrid) chunkGrid).chunkShape;
  }

  public ChunkKeyEncoding getChunkKeyEncoding() {
    return chunkKeyEncoding;
  }

  public ChunkGrid getChunkGrid() {
    return chunkGrid;
  }

  public Zarr3CodecPipeline getCodecs() {

    return codecs;
  }

  public DataType getDType() {

    return dataType;
  }

  public int getZarrFormat() {

    return Zarr3KeyValueReader.VERSION.getMajor();
  }

  public Object getFillValue() {

    return fillValue;
  }

  public JsonObject getAttributes() {

    return attributes;
  }

  public HashMap<String, Object> asMap() {

    final HashMap<String, Object> map = new HashMap<>();

    map.put(Zarr3KeyValueReader.ZARR_FORMAT_KEY, Zarr3KeyValueReader.VERSION.getMajor());
    map.put(Zarr3KeyValueReader.NODE_TYPE_KEY, Zarr3KeyValueReader.NODE_TYPE_ARRAY);
    map.put(shapeKey, shape);
    map.put(dataTypeKey, dataType.toString());
    map.put(chunkGridKey, chunkGrid.asMap());
    map.put(chunkKeyEncodingKey, chunkKeyEncoding.asMap());
    map.put(fillValueKey, fillValue);
    map.put(codecsKey, codecs.getCodecs());
    map.put(attributesKey, attributes);

    return map;
  }

  public static class JsonAdapter implements JsonDeserializer<Zarr3ArrayAttributes> {

    @Override
    public Zarr3ArrayAttributes deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {

      final JsonObject obj = json.getAsJsonObject();
      try {
        final String typestr = obj.get("data_type").getAsString();
        final DataType dataType = new DataType(typestr);

        final JsonPrimitive fillValueJson = obj.get("fill_value").getAsJsonPrimitive();
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

        return new Zarr3ArrayAttributes(
            context.deserialize(obj.get("shape"), long[].class),
            dataType,
            context.deserialize(obj.get("chunk_grid"), ChunkGrid.class),
            context.deserialize(obj.get("chunk_key_encoding"), ChunkKeyEncoding.class),
            context.deserialize(obj.get("codecs"), Zarr3CodecPipeline.class), // fix
            fillValue,
            obj.get("attributes").getAsJsonObject()
        );
      } catch (Exception e) {
        return null;
      }
    }

  }
}
