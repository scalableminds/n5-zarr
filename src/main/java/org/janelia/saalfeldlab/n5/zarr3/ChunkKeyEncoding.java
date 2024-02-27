package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.N5Exception;

public interface ChunkKeyEncoding {

  public static Map<String, Class<? extends ChunkKeyEncoding>> registry = Stream.of(
          new SimpleImmutableEntry<>("v2", V2ChunkKeyEncoding.class),
          new SimpleImmutableEntry<>("default", DefaultChunkKeyEncoding.class)
      )
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  public static JsonAdapter jsonAdapter = new JsonAdapter();

  String encodeChunkKey(long[] chunkCoords);
  Map<String, Object> asMap();

  public static class V2ChunkKeyEncoding implements ChunkKeyEncoding {

    public static V2JsonAdapter jsonAdapter = new V2JsonAdapter();
    public final char separator;

    public V2ChunkKeyEncoding(char separator) {
      if (separator == '.' || separator == '/') {
        this.separator = separator;
      } else {
        throw new N5Exception("Invalid separator: " + separator);
      }
    }

    public String encodeChunkKey(long[] chunkCoords) {
      Stream<String> keys = Arrays.stream(chunkCoords)
          .mapToObj(Long::toString);
      return keys.collect(Collectors.joining(String.valueOf(separator)));
    }

    public Map<String, Object> asMap() {
      return Stream.of(
              new SimpleImmutableEntry<>("name", "v2"),
              new SimpleImmutableEntry<>("configuration", Stream.of(
                      new SimpleImmutableEntry<>("separator", separator)
                  )
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
          )
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof V2ChunkKeyEncoding
          && ((V2ChunkKeyEncoding) o).separator == separator;
    }

    static public class V2JsonAdapter implements JsonDeserializer<V2ChunkKeyEncoding> {

      @Override
      public V2ChunkKeyEncoding deserialize(final JsonElement json, final Type typeOfT,
          final JsonDeserializationContext context)
          throws JsonParseException {

        final JsonObject jsonObject = json.getAsJsonObject();
        final JsonObject jsonConfigurationObject = jsonObject.get("configuration")
            .getAsJsonObject();
        final char separator = jsonConfigurationObject.get("separator").getAsString().charAt(0);
        return new V2ChunkKeyEncoding(separator);
      }
    }
  }

  public static class DefaultChunkKeyEncoding implements ChunkKeyEncoding {

    public static DefaultJsonAdapter jsonAdapter = new DefaultJsonAdapter();
    public final char separator;

    public DefaultChunkKeyEncoding(char separator) {
      if (separator == '.' || separator == '/') {
        this.separator = separator;
      } else {
        throw new N5Exception("Invalid separator: " + separator);
      }
    }

    public String encodeChunkKey(long[] chunkCoords) {
      Stream<String> keys = Stream.concat(Stream.of("c"), Arrays.stream(chunkCoords)
          .mapToObj(Long::toString));
      return keys.collect(Collectors.joining(String.valueOf(separator)));
    }

    public Map<String, Object> asMap() {
      return Stream.of(
              new SimpleImmutableEntry<>("name", "default"),
              new SimpleImmutableEntry<>("configuration", Stream.of(
                      new SimpleImmutableEntry<>("separator", separator)
                  )
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
          )
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof DefaultChunkKeyEncoding
          && ((DefaultChunkKeyEncoding) o).separator == separator;
    }

    static public class DefaultJsonAdapter implements JsonDeserializer<DefaultChunkKeyEncoding> {

      @Override
      public DefaultChunkKeyEncoding deserialize(final JsonElement json, final Type typeOfT,
          final JsonDeserializationContext context)
          throws JsonParseException {

        final JsonObject jsonObject = json.getAsJsonObject();
        final JsonObject jsonConfigurationObject = jsonObject.get("configuration")
            .getAsJsonObject();
        final char separator = jsonConfigurationObject.get("separator").getAsString().charAt(0);
        return new DefaultChunkKeyEncoding(separator);
      }
    }
  }

  static public class JsonAdapter implements JsonDeserializer<ChunkKeyEncoding> {

    @Override
    public ChunkKeyEncoding deserialize(final JsonElement json, final Type typeOfT,
        final JsonDeserializationContext context)
        throws JsonParseException {

      final JsonObject jsonObject = json.getAsJsonObject();
      final JsonElement jsonName = jsonObject.get("name");
      if (jsonName == null) {
        return null;
      }
      final String name = jsonName.getAsString();
      final Class<? extends ChunkKeyEncoding> chunkKeyEncodingClass = registry.get(name);
      if (chunkKeyEncodingClass == null) {
        return null;
      }

      return context.deserialize(json, chunkKeyEncodingClass);
    }
  }
}
