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
import java.nio.ByteBuffer;
import org.apache.commons.compress.utils.IOUtils;
import org.blosc.BufferSizes;
import org.blosc.JBlosc;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.DefaultBlockWriter;
import org.janelia.saalfeldlab.n5.zarr3.ZarrCodec.JsonAdapter;

public class Zarr3CodecPipeline implements DefaultBlockReader, DefaultBlockWriter, Compression {

  private JsonArray codecs;

  public static JsonAdapter jsonAdapter = new JsonAdapter();

  public Zarr3CodecPipeline(JsonArray codecs) {
    this.codecs = codecs;
  }

  public <T, B extends DataBlock<T>> void read(B dataBlock, InputStream in) throws IOException {
    ((DefaultBlockReader)this).read(dataBlock, in);
  }

  public <T> void write(DataBlock<T> dataBlock, OutputStream out) throws IOException {
    ((DefaultBlockWriter)this).write(dataBlock, out);
  }

  public Zarr3CodecPipeline getReader() {
    return this;
  }

  public Zarr3CodecPipeline getWriter() {
    return this;
  }

  public OutputStream getOutputStream(OutputStream out) throws IOException {
    return null;
  }

  public InputStream getInputStream(InputStream in) throws IOException {
    return null;
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