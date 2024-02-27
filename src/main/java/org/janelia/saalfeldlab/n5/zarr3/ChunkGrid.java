package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ChunkGrid {

  public static Map<String, Class<? extends ChunkGrid>> registry = Stream.of(
          new SimpleImmutableEntry<>("regular", RegularChunkGrid.class)
      )
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  public static JsonAdapter jsonAdapter = new JsonAdapter();

  Map<String, Object> asMap();

  public static class RegularChunkGrid implements ChunkGrid {

    public static RegularJsonAdapter jsonAdapter = new RegularJsonAdapter();
    public final int[] chunkShape;

    public RegularChunkGrid(int[] chunkShape) {
      this.chunkShape = chunkShape;
    }

    public Map<String, Object> asMap() {
      return Stream.of(
              new SimpleImmutableEntry<>("name", "regular"),
              new SimpleImmutableEntry<>("configuration", Stream.of(
                      new SimpleImmutableEntry<>("chunk_shape", chunkShape)
                  )
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
          )
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static public class RegularJsonAdapter implements JsonDeserializer<RegularChunkGrid> {

      @Override
      public RegularChunkGrid deserialize(final JsonElement json, final Type typeOfT,
          final JsonDeserializationContext context)
          throws JsonParseException {

        final JsonObject jsonObject = json.getAsJsonObject();
        final JsonObject jsonConfigurationObject = jsonObject.get("configuration")
            .getAsJsonObject();
        final int[] chunkShape = context.deserialize(jsonConfigurationObject.get("chunk_shape"),
            int[].class);
        return new RegularChunkGrid(chunkShape);
      }
    }
  }

  static public class JsonAdapter implements JsonDeserializer<ChunkGrid> {

    @Override
    public ChunkGrid deserialize(final JsonElement json, final Type typeOfT,
        final JsonDeserializationContext context)
        throws JsonParseException {

      final JsonObject jsonObject = json.getAsJsonObject();
      final JsonElement jsonName = jsonObject.get("name");
      if (jsonName == null) {
        return null;
      }
      final String name = jsonName.getAsString();
      final Class<? extends ChunkGrid> chunkGridClass = registry.get(name);
      if (chunkGridClass == null) {
        return null;
      }

      return context.deserialize(json, chunkGridClass);
    }
  }
}
