package org.janelia.saalfeldlab.n5.zarr3;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

public class JsonBuilder {

  JsonObject obj;

  JsonBuilder() {
    obj = new JsonObject();
  }

  public static JsonBuilder object() {
    return new JsonBuilder();
  }

  public static JsonArrayBuilder array() {
    return new JsonArrayBuilder();
  }

  public static JsonPrimitive primitive(String value) {
    return new JsonPrimitive(value);
  }

  public static JsonPrimitive primitive(Number value) {
    return new JsonPrimitive(value);
  }

  public static JsonPrimitive primitive(Boolean value) {
    return new JsonPrimitive(value);
  }

  public static JsonPrimitive primitive(Character value) {
    return new JsonPrimitive(value);
  }

  public JsonBuilder addProperty(String key, String value) {
    obj.addProperty(key, value);
    return this;
  }

  public JsonBuilder addProperty(String key, Number value) {
    obj.addProperty(key, value);
    return this;
  }

  public JsonBuilder addProperty(String key, Boolean value) {
    obj.addProperty(key, value);
    return this;
  }

  public JsonBuilder addProperty(String key, Character value) {
    obj.addProperty(key, value);
    return this;
  }

  public JsonBuilder addProperty(String key, JsonElement value) {
    obj.add(key, value);
    return this;
  }

  public JsonBuilder addObject(String key, Function<JsonBuilder, JsonBuilder> func) {
    obj.add(key, func.apply(new JsonBuilder()).build());
    return this;
  }

  public JsonBuilder addObject(String key, JsonObject value) {
    obj.add(key, value);
    return this;
  }

  public JsonBuilder addArray(String key, Function<JsonArrayBuilder, JsonArrayBuilder> func) {
    obj.add(key, func.apply(new JsonArrayBuilder()).build());
    return this;
  }

  public JsonBuilder addArray(String key, JsonArray value) {
    obj.add(key, value);
    return this;
  }

  public JsonBuilder addArray(String key, long[] arr) {
    JsonArrayBuilder jsonArrayBuilder = array();
    for (long a : arr) {
      jsonArrayBuilder.add(a);
    }
    obj.add(key, jsonArrayBuilder.build());
    return this;
  }


  public JsonBuilder addArray(String key, int[] arr) {
    JsonArrayBuilder jsonArrayBuilder = array();
    for (int a : arr) {
      jsonArrayBuilder.add(a);
    }
    obj.add(key, jsonArrayBuilder.build());
    return this;
  }


  public JsonObject build() {
    return obj;
  }



  public static class JsonArrayBuilder {

    JsonArray arr;

    JsonArrayBuilder() {
      arr = new JsonArray();
    }

    public JsonArrayBuilder addObject(Function<JsonBuilder, JsonBuilder> func) {
      arr.add(func.apply(new JsonBuilder()).build());
      return this;
    }

    public JsonArrayBuilder addObject(JsonObject value) {
      arr.add(value);
      return this;
    }

    public JsonArrayBuilder addArray(Function<JsonArrayBuilder, JsonArrayBuilder> func) {
      arr.add(func.apply(new JsonArrayBuilder()).build());
      return this;
    }

    public JsonArrayBuilder addArray(JsonArray value) {
      arr.add(value);
      return this;
    }

    public JsonArrayBuilder add(String value) {
      arr.add(value);
      return this;
    }

    public JsonArrayBuilder add(Number value) {
      arr.add(value);
      return this;
    }

    public JsonArrayBuilder add(Boolean value) {
      arr.add(value);
      return this;
    }

    public JsonArrayBuilder add(Character value) {
      arr.add(value);
      return this;
    }

    public JsonArrayBuilder add(JsonElement value) {
      arr.add(value);
      return this;
    }

    public JsonArray build() {
      return arr;
    }
  }
}
