package org.janelia.saalfeldlab.n5.zarr3;

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
}
