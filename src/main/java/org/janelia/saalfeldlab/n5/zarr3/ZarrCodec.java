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
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.blosc.BloscCompression;
import org.janelia.scicomp.n5.zstandard.ZstandardCompression;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public interface ZarrCodec {

  /* idiotic stream based initialization because Java cannot have static initialization code in interfaces */
  public static Map<String, Class<? extends ZarrCodec>> registry = Stream.of(
          new SimpleImmutableEntry<>("zstd", Zstandard.class),
          new SimpleImmutableEntry<>("blosc", Blosc.class),
          new SimpleImmutableEntry<>("gzip", Gzip.class),
          new SimpleImmutableEntry<>("bytes", Raw.class),
          new SimpleImmutableEntry<>("transpose", Raw.class),
          new SimpleImmutableEntry<>("sharding_indexed", Raw.class)
      )
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  public static JsonAdapter jsonAdapter = new JsonAdapter();

  public static ZarrCodec fromCompression(final Compression compression) {
    try {
      if (compression instanceof BloscCompression) {
        return new Blosc((BloscCompression) compression);
      } else if (compression instanceof GzipCompression) {
        return new Gzip((GzipCompression) compression);
      } else if (compression instanceof ZstandardCompression) {
        return new Zstandard((ZstandardCompression) compression);
      } else {
        return new Raw();
      }
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException |
             IllegalAccessException e) {
      return null;
    }
  }

  public Compression getCompression();

  public static class Zstandard implements ZarrCodec {

    @SuppressWarnings("unused")
    private final String name = "zstd";
    private final int level;
    private final boolean checksum;
    private final transient int nbWorkers;

    public Zstandard(int level) {
      this(level, false);
    }

    public Zstandard(int level, boolean checksum) {
      this(level, checksum, 0);
    }

    public Zstandard(int level, boolean checksum, int nbWorkers) {
      this.level = level;
      this.checksum = checksum;
      this.nbWorkers = nbWorkers;
    }

    public Zstandard(ZstandardCompression compression) {
      this.level = compression.getLevel();
      this.checksum = compression.isUseChecksums();
      this.nbWorkers = compression.getNbWorkers();
    }

    @Override
    public Compression getCompression() {
      ZstandardCompression compression = new ZstandardCompression(level);
      if (this.nbWorkers != 0) {
        compression.setNbWorkers(this.nbWorkers);
      }
      return compression;
    }

  }

  public static class Blosc implements ZarrCodec {

    @SuppressWarnings("unused")
    private final String name = "blosc";
    private final String cname;
    private final int clevel;
    private final Shuffle shuffle;
    private final int blocksize;
    private final transient int nthreads;

    public Blosc(
        final String cname,
        final int clevel,
        final Shuffle shuffle,
        final int blockSize,
        final int nthreads) {

      this.cname = cname;
      this.clevel = clevel;
      this.shuffle = shuffle;
      this.blocksize = blockSize;
      this.nthreads = nthreads;
    }

    public Blosc(final BloscCompression compression)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

      final Class<? extends BloscCompression> clazz = compression.getClass();

      Field field = clazz.getDeclaredField("cname");
      field.setAccessible(true);
      cname = (String) field.get(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("clevel");
      field.setAccessible(true);
      clevel = field.getInt(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("shuffle");
      field.setAccessible(true);
      shuffle = Shuffle.fromInt(field.getInt(compression));
      field.setAccessible(false);

      field = clazz.getDeclaredField("blocksize");
      field.setAccessible(true);
      blocksize = field.getInt(compression);
      field.setAccessible(false);

      field = clazz.getDeclaredField("nthreads");
      field.setAccessible(true);
      nthreads = field.getInt(compression);
      field.setAccessible(false);
    }

    @Override
    public BloscCompression getCompression() {

      return new BloscCompression(cname, clevel, shuffle.getValue(), blocksize,
          Math.max(1, nthreads));
    }

    public enum Shuffle {
      /**
       * Disable shuffling
       */
      NO_SHUFFLE(0, "noshuffle"),
      /**
       * Byte-wise shuffling
       */
      BYTE_SHUFFLE(1, "byteshuffle"),
      /**
       * Bit-wise shuffling
       */
      BIT_SHUFFLE(2, "bitshuffle");
      private final int shuffle;
      private final String shuffleString;

      Shuffle(int shuffle, String shuffleString) {
        this.shuffle = shuffle;
        this.shuffleString = shuffleString;
      }

      public static Shuffle fromInt(int shuffle) {
        for (Shuffle e : values()) {
          if (e.shuffle == shuffle) {
            return e;
          }
        }
        return null;
      }

      public static Shuffle fromString(String shuffle) {
        for (Shuffle e : values()) {
          if (e.shuffleString.equalsIgnoreCase(shuffle)) {
            return e;
          }
        }
        return null;
      }

      public int getValue() {
        return shuffle;
      }
    }
  }


  public static class Gzip implements ZarrCodec {

    @SuppressWarnings("unused")
    private final String name = "gzip";
    private final int level;

    public Gzip(final int level) {

      this.level = level;
    }

    public Gzip(final GzipCompression compression)
        throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {

      final Class<? extends GzipCompression> clazz = compression.getClass();

      final Field field = clazz.getDeclaredField("level");
      field.setAccessible(true);
      level = field.getInt(compression);
      field.setAccessible(false);
    }

    @Override
    public GzipCompression getCompression() {

      return new GzipCompression(level);
    }
  }

  public static class Raw extends RawCompression implements ZarrCodec {

    @Override
    public RawCompression getCompression() {

      return this;
    }
  }

  static public class JsonAdapter implements JsonDeserializer<ZarrCodec> {

    @Override
    public ZarrCodec deserialize(final JsonElement json, final Type typeOfT,
        final JsonDeserializationContext context)
        throws JsonParseException {

      final JsonObject jsonObject = json.getAsJsonObject();
      final JsonElement jsonName = jsonObject.get("name");
      if (jsonName == null) {
        return null;
      }
      final String name = jsonName.getAsString();
      final Class<? extends ZarrCodec> codecClass = registry.get(name);
      if (codecClass == null) {
        return null;
      }

      return context.deserialize(json, codecClass);
    }
  }
}
