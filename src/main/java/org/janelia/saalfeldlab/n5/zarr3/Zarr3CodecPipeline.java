package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.RawCompression;

public class Zarr3CodecPipeline implements DefaultBlockReader, DefaultBlockWriter, Compression {

  public static JsonAdapter jsonAdapter = new JsonAdapter();
  private JsonArray codecs;

  public Zarr3CodecPipeline(JsonArray codecs) {
    this.codecs = codecs;
  }

  public static Zarr3CodecPipeline guessCompression(Compression compression) {
    JsonObject bytesCodec = JsonBuilder.object()
        .addProperty("name", "bytes")
        .addObject("configuration", b1 -> b1.addProperty("endian", "little"))
        .build();
    if (compression instanceof RawCompression) {
      return new Zarr3CodecPipeline(JsonBuilder.array().addObject(bytesCodec).build());
    } else if (compression instanceof GzipCompression) {
      return new Zarr3CodecPipeline(JsonBuilder.array()
          .addObject(bytesCodec)
          .addObject(c -> c
              .addProperty("name", "gzip")
              .addObject("configuration", c1 -> c1.addProperty("level", 5)))
          .build());
    } else if (compression instanceof Zarr3CodecPipeline) {
      return (Zarr3CodecPipeline) compression;
    }
    throw new RuntimeException("Sorry about that. " + compression.toString());
  }

  /*
  public <T, B extends DataBlock<T>> void read(B dataBlock, InputStream in) throws IOException {
    ((DefaultBlockReader)this).read(dataBlock, in);
  }

  public <T> void write(DataBlock<T> dataBlock, OutputStream out) throws IOException {
    ((DefaultBlockWriter)this).write(dataBlock, out);
  }*/

  public Zarr3CodecPipeline getReader() {
    return this;
  }

  public Zarr3CodecPipeline getWriter() {
    return this;
  }

  public OutputStream getOutputStream(OutputStream out) throws IOException {
    return out;
  }

  public InputStream getInputStream(InputStream in) throws IOException {
    return in;
  }

  public JsonArray getCodecs() {
    return codecs;
  }

  static public class JsonAdapter implements JsonDeserializer<Zarr3CodecPipeline> {

    @Override
    public Zarr3CodecPipeline deserialize(final JsonElement json, final Type typeOfT,
        final JsonDeserializationContext context)
        throws JsonParseException {

      return new Zarr3CodecPipeline(json.getAsJsonArray());
    }
  }
}