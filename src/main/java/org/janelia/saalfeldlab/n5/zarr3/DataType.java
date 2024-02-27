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

import java.nio.ByteBuffer;
import java.util.EnumMap;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;

/**
 * Enumerates available zarr data types as defined at
 * https://docs.scipy.org/doc/numpy/reference/arrays.interface.html
 * <p>
 * At this time, only primitive types are supported, no recursive structs. This is compatible with
 * the upcoming Zarr spec v3 core types plus extras.
 *
 * @author Stephan Saalfeld
 */
public class DataType {

  private static final EnumMap<org.janelia.saalfeldlab.n5.DataType, String> typestrs = new EnumMap<>(
      org.janelia.saalfeldlab.n5.DataType.class);
  /* According to https://docs.scipy.org/doc/numpy/reference/arrays.interface.html
   * this should be the number of bytes per scalar except for BIT where it is the
   * number of bits.  Also not sure what the quantifier means for strings, times,
   * and objects
   */
  protected final int nBytes;
  protected final ByteBlockFactory byteBlockFactory;
  protected final DataBlockFactory dataBlockFactory;
  /* the closest possible N5 DataType */
  protected final org.janelia.saalfeldlab.n5.DataType dataType;
  protected String typestr;

  {
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.INT8, "int8");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.UINT8, "uint8");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.INT16, "int16");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.UINT16, "uint16");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.INT32, "int32");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.UINT32, "uint32");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.INT64, "int64");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.UINT64, "uint64");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.FLOAT32, "float32");
    typestrs.put(org.janelia.saalfeldlab.n5.DataType.FLOAT64, "float64");
  }


  public DataType(final String typestr) {
    this.typestr = typestr;
    switch (typestr) {
      case "bool":
      case "uint8":
        dataType = org.janelia.saalfeldlab.n5.DataType.UINT8;
        nBytes = 1;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements]);
        break;
      case "int8":
        dataType = org.janelia.saalfeldlab.n5.DataType.INT8;
        nBytes = 1;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements]);
        break;
      case "uint16":
        dataType = org.janelia.saalfeldlab.n5.DataType.UINT16;
        nBytes = 2;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new ShortArrayDataBlock(blockSize, gridPosition, new short[numElements]);
        break;
      case "int16":
        dataType = org.janelia.saalfeldlab.n5.DataType.INT16;
        nBytes = 2;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new ShortArrayDataBlock(blockSize, gridPosition, new short[numElements]);
        break;
      case "uint32":
        dataType = org.janelia.saalfeldlab.n5.DataType.UINT32;
        nBytes = 4;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new IntArrayDataBlock(blockSize, gridPosition, new int[numElements]);
        break;
      case "int32":
        dataType = org.janelia.saalfeldlab.n5.DataType.INT32;
        nBytes = 4;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new IntArrayDataBlock(blockSize, gridPosition, new int[numElements]);
        break;
      case "uint64":
        dataType = org.janelia.saalfeldlab.n5.DataType.UINT64;
        nBytes = 8;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new LongArrayDataBlock(blockSize, gridPosition, new long[numElements]);
        break;
      case "int64":
        dataType = org.janelia.saalfeldlab.n5.DataType.INT64;
        nBytes = 8;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new LongArrayDataBlock(blockSize, gridPosition, new long[numElements]);
        break;
      case "float32":
        dataType = org.janelia.saalfeldlab.n5.DataType.FLOAT32;
        nBytes = 4;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new FloatArrayDataBlock(blockSize, gridPosition, new float[numElements]);
        break;
      case "float64":
        dataType = org.janelia.saalfeldlab.n5.DataType.FLOAT64;
        nBytes = 8;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new DoubleArrayDataBlock(blockSize, gridPosition, new double[numElements]);
        break;
      default:
        throw new N5Exception(String.format("Invalid data_type: %s", typestr));
    }
    byteBlockFactory = (blockSize, gridPosition, numElements) ->
        new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements * nBytes]);
  }

  public DataType(final org.janelia.saalfeldlab.n5.DataType dataType) {

    typestr = typestrs.get(dataType);
    this.dataType = dataType;

    switch (dataType) {
      case INT8:
      case UINT8:
        nBytes = 1;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements]);
        break;
      case INT16:
      case UINT16:
        nBytes = 2;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new ShortArrayDataBlock(blockSize, gridPosition, new short[numElements]);
        break;
      case INT32:
      case UINT32:
        nBytes = 4;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new IntArrayDataBlock(blockSize, gridPosition, new int[numElements]);
        break;
      case INT64:
      case UINT64:
        nBytes = 8;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new LongArrayDataBlock(blockSize, gridPosition, new long[numElements]);
        break;
      case FLOAT32:
        nBytes = 4;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new FloatArrayDataBlock(blockSize, gridPosition, new float[numElements]);
        break;
      case FLOAT64:
        nBytes = 8;
        dataBlockFactory = (blockSize, gridPosition, numElements) ->
            new DoubleArrayDataBlock(blockSize, gridPosition,
                new double[numElements]);
        break;
      default:
        throw new N5Exception("Unreachable");
    }
    byteBlockFactory = (blockSize, gridPosition, numElements) ->
        new ByteArrayDataBlock(blockSize, gridPosition, new byte[numElements * nBytes]);
  }

  public byte[] createFillBytes(Object fillValue) {
    final byte[] fillBytes = new byte[nBytes];
    final ByteBuffer fillBuffer = ByteBuffer.wrap(fillBytes);

    if (fillValue instanceof Boolean) {
      Boolean fillValueBool = (Boolean) fillValue;
      if (typestr.equals("bool")) {
        fillBuffer.put((byte) (fillValueBool ? 1 : 0));
        return fillBytes;
      }
    }
    if (fillValue instanceof Number) {
      Number fillValueNumber = (Number) fillValue;
      switch (typestr) {
        case "bool":
          fillBuffer.put((byte) (fillValueNumber.byteValue() != 0 ? 1 : 0));
          return fillBytes;
        case "int8":
        case "uint8":
          fillBuffer.put(fillValueNumber.byteValue());
          return fillBytes;
        case "int16":
        case "uint16":
          fillBuffer.putShort(fillValueNumber.shortValue());
          return fillBytes;
        case "int32":
        case "uint32":
          fillBuffer.putInt(fillValueNumber.intValue());
          return fillBytes;
        case "int64":
        case "uint64":
          fillBuffer.putLong(fillValueNumber.longValue());
          return fillBytes;
        case "float32":
          fillBuffer.putFloat(fillValueNumber.floatValue());
          return fillBytes;
        case "float64":
          fillBuffer.putDouble(fillValueNumber.doubleValue());
          return fillBytes;
        default:
          // Fallback to throwing below
      }
    } else if (fillValue instanceof String) {
      String fillValueString = (String) fillValue;
      if (fillValueString.equals("NaN")) {
        switch (typestr) {
          case "float32":
            fillBuffer.putFloat(Float.NaN);
            return fillBytes;
          case "float64":
            fillBuffer.putDouble(Double.NaN);
            return fillBytes;
          default:
            throw new N5Exception(
                "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.equals("+Infinity")) {
        switch (typestr) {
          case "float32":
            fillBuffer.putFloat(Float.POSITIVE_INFINITY);
            return fillBytes;
          case "float64":
            fillBuffer.putDouble(Double.POSITIVE_INFINITY);
            return fillBytes;
          default:
            throw new N5Exception(
                "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.equals("-Infinity")) {
        switch (typestr) {
          case "float32":
            fillBuffer.putFloat(Float.NEGATIVE_INFINITY);
            return fillBytes;
          case "float64":
            fillBuffer.putDouble(Double.NEGATIVE_INFINITY);
            return fillBytes;
          default:
            throw new N5Exception(
                "Invalid fill value '" + fillValueString + "' for data type '" + dataType + "'.");
        }
      } else if (fillValueString.startsWith("0b") || fillValueString.startsWith("0x")) {
        if (fillValueString.startsWith("0b")) {

          for (int i = 0; i < nBytes; i++) {
            fillBuffer.put(
                (byte) Integer.parseInt(fillValueString.substring(2 + i * 8, 2 + (i + 1) * 8),
                    2));
          }
        } else if (fillValueString.startsWith("0x")) {
          for (int i = 0; i < nBytes; i++) {
            fillBuffer.put(
                (byte) Integer.parseInt(fillValueString.substring(2 + i * 2, 2 + (i + 1) * 2),
                    16));
          }
        }
      }
    }
    throw new N5Exception("Invalid fill value '" + fillValue + "'.");
  }

  public org.janelia.saalfeldlab.n5.DataType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {

    return typestr;
  }

  /**
   * Factory for {@link DataBlock DataBlocks}.
   *
   * @param blockSize
   * @param gridPosition
   * @param numElements  not necessarily one element per block element
   * @return
   */
  public DataBlock<?> createDataBlock(final int[] blockSize, final long[] gridPosition,
      final int numElements) {

    return dataBlockFactory.createDataBlock(blockSize, gridPosition, numElements);
  }

  /**
   * Factory for {@link ByteArrayDataBlock ByteArrayDataBlocks}.
   *
   * @param blockSize
   * @param gridPosition
   * @param numElements  not necessarily one element per block element
   * @return
   */
  public ByteArrayDataBlock createByteBlock(final int[] blockSize, final long[] gridPosition,
      final int numElements) {

    return byteBlockFactory.createByteBlock(blockSize, gridPosition, numElements);
  }

  /**
   * Factory for {@link DataBlock DataBlocks} with one data element for each block element (e.g.
   * pixel image).
   *
   * @param blockSize
   * @param gridPosition
   * @return
   */
  public DataBlock<?> createDataBlock(final int[] blockSize, final long[] gridPosition) {

    return dataBlockFactory.createDataBlock(blockSize, gridPosition,
        DataBlock.getNumElements(blockSize));
  }

  /**
   * Factory for {@link ByteArrayDataBlock ByteArrayDataBlocks}.
   *
   * @param blockSize
   * @param gridPosition
   * @return
   */
  public ByteArrayDataBlock createByteBlock(final int[] blockSize, final long[] gridPosition) {

    return byteBlockFactory.createByteBlock(blockSize, gridPosition,
        DataBlock.getNumElements(blockSize));
  }

  /**
   * Returns the number of bytes of this type.  If the type counts the number of bits, this method
   * returns 0.
   *
   * @return number of bytes
   */
  public int getNBytes() {

    return nBytes;
  }

  public Object defaultFillValue() {
    switch (typestr) {
      case "bool":
        return false;
      case "int8":
      case "uint8":
        return (byte)0;
      case "int16":
      case "uint16":
        return (short)0;
      case "int32":
      case "uint32":
        return (int)0;
      case "int64":
      case "uint64":
        return (long)0;
      case "float32":
        return (float)0;
      case "float64":
        return (double)0;
      default:
        // Fallback to throwing below
    }
    throw new RuntimeException("Unreachable");
  }

  private static interface DataBlockFactory {

    public DataBlock<?> createDataBlock(final int[] blockSize, final long[] gridPosition,
        final int numElements);
  }

  private static interface ByteBlockFactory {

    public ByteArrayDataBlock createByteBlock(final int[] blockSize, final long[] gridPosition,
        final int numElements);
  }
}
