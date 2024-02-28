package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

public class Utils {
  public static ByteBuffer makeByteBuffer(int capacity, Function<ByteBuffer, ByteBuffer> func) {
    ByteBuffer buf = ByteBuffer.allocate(capacity)
        .order(ByteOrder.LITTLE_ENDIAN);
    buf = func.apply(buf);
    buf.rewind();
    return buf;
  }

  public static JsonArray toJsonArray(int[] arr) {
    JsonArray jsonArray = new JsonArray();
    for (int a : arr) {
      jsonArray.add(new JsonPrimitive(a));
    }
    return jsonArray;
  }
  public static JsonArray toJsonArray(long[] arr) {
    JsonArray jsonArray = new JsonArray();
    for (long a : arr) {
      jsonArray.add(new JsonPrimitive(a));
    }
    return jsonArray;
  }
}
