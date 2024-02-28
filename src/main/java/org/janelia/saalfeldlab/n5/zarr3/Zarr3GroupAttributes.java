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
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Objects;


/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public class Zarr3GroupAttributes {

  public static final String attributesKey = "attributes";

  public static final String[] requiredKeys = new String[]{
      Zarr3KeyValueReader.ZARR_FORMAT_KEY,
  };

  public static final String[] allKeys = new String[]{Zarr3KeyValueReader.ZARR_FORMAT_KEY,
      Zarr3KeyValueReader.NODE_TYPE_KEY, attributesKey
  };
  public static JsonAdapter jsonAdapter = new JsonAdapter();

  private final JsonObject attributes;

  public Zarr3GroupAttributes() {
    this(new JsonObject());
  }

  public Zarr3GroupAttributes(final JsonObject attributes) {
    this.attributes = attributes;
  }


  public int getZarrFormat() {

    return Zarr3KeyValueReader.VERSION.getMajor();
  }

  public JsonObject getAttributes() {

    return attributes;
  }

  public HashMap<String, Object> asMap() {

    final HashMap<String, Object> map = new HashMap<>();

    map.put(Zarr3KeyValueReader.ZARR_FORMAT_KEY, Zarr3KeyValueReader.VERSION.getMajor());
    map.put(Zarr3KeyValueReader.NODE_TYPE_KEY, Zarr3KeyValueReader.NODE_TYPE_GROUP);
    map.put(attributesKey, attributes);

    return map;
  }

  public static class JsonAdapter implements JsonDeserializer<Zarr3GroupAttributes> {

    @Override
    public Zarr3GroupAttributes deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {

      final JsonObject obj = json.getAsJsonObject();
      try {
        if (!Objects.equals(obj.get(Zarr3KeyValueReader.NODE_TYPE_KEY).getAsString(),
            Zarr3KeyValueReader.NODE_TYPE_GROUP)) {
          return null;
        }
        return new Zarr3GroupAttributes(
            obj.get("attributes").getAsJsonObject()
        );
      } catch (Exception e) {
        return null;
      }
    }

  }
}
