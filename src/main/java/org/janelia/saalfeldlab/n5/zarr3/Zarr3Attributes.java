package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import java.lang.reflect.Type;
import java.util.Map;
import org.janelia.saalfeldlab.n5.GsonUtils;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader.Version;
import org.janelia.saalfeldlab.n5.N5URI;

public interface Zarr3Attributes {


  public static JsonAdapter jsonAdapter = new JsonAdapter();
  final String NODE_TYPE_KEY = "node_type";
  final String NODE_TYPE_GROUP = "group";
  final String NODE_TYPE_ARRAY = "array";
  final String ZARR_FORMAT_KEY = "zarr_format";
  final Version ZARR_3 = new Version(3,0,0);

  public static Zarr3Attributes fromJson(final JsonElement jsonElement, Gson gson) {
    return gson.fromJson(jsonElement, Zarr3Attributes.class);
  }

  Zarr3Attributes setAttributes(final Map<String, ?> attributes, Gson gson);

  default <V> V getAttribute(final String keyPath, Gson gson, final Class<V> clazz) {
    final String normalizedAttributePath = N5URI.normalizeAttributePath(keyPath);
    try {
      return GsonUtils.readAttribute(asN5Attributes(gson), normalizedAttributePath, clazz, gson);
    } catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
  }

  default <V> V getAttribute(final String keyPath, Gson gson, final Type type) {
    final String normalizedAttributePath = N5URI.normalizeAttributePath(keyPath);
    try {
      return GsonUtils.readAttribute(asN5Attributes(gson), normalizedAttributePath, type, gson);
    } catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
  }

  default JsonElement getAttribute(final String keyPath, Gson gson) {
    final String normalizedAttributePath = N5URI.normalizeAttributePath(keyPath);
    try {
      return GsonUtils.getAttribute(asN5Attributes(gson), normalizedAttributePath);
    } catch (JsonSyntaxException | NumberFormatException | ClassCastException e) {
      throw new N5Exception.N5ClassCastException(e);
    }
  }

  Zarr3Attributes removeAttribute(final String keyPath, Gson gson);

  Map<String, Object> asMap();

  default JsonElement asJson(Gson gson) {
      return gson.toJsonTree(asMap()).getAsJsonObject();
  }

  default Version getZarrFormat() {
    return ZARR_3;
  }

  JsonObject asN5Attributes(Gson gson);

  static public class JsonAdapter implements
      JsonDeserializer<Zarr3Attributes> {

    @Override
    public Zarr3Attributes deserialize(final JsonElement json,
        final Type typeOfT,
        final JsonDeserializationContext context)
        throws JsonParseException {

      final JsonObject jsonObject = json.getAsJsonObject();
      final String nodeType = jsonObject.get(NODE_TYPE_KEY).getAsString();
      switch (nodeType) {
        case NODE_TYPE_ARRAY:
          return context.deserialize(json, Zarr3DatasetAttributes.class);
        case NODE_TYPE_GROUP:
          return context.deserialize(json, Zarr3GroupAttributes.class);
        default:
          return null;
      }
    }
  }
}
