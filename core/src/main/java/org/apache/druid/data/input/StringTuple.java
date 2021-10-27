/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.io.IOException;
import java.util.Arrays;

/**
 * Represents a tuple of String values, typically used to represent
 * (single-valued) dimension values for an InputRow.
 */
@JsonSerialize(using = StringTuple.Serializer.class)
@JsonDeserialize(using = StringTuple.Deserializer.class)
public class StringTuple implements Comparable<StringTuple>
{

  @JsonProperty("values")
  private final String[] values;

  public static StringTuple create(String... values)
  {
    return new StringTuple(values);
  }

  @JsonCreator
  private StringTuple(
      @JsonProperty("values") String... values
  )
  {
    Preconditions.checkNotNull(values, "Array of values should not be null");
    this.values = values;
  }

  public String get(int index)
  {
    return values[index];
  }

  public int size()
  {
    return values.length;
  }

  public String[] toArray()
  {
    return Arrays.copyOf(values, size());
  }

  @Override
  public int compareTo(StringTuple that)
  {
    // null is less than non-null
    if (this == that) {
      return 0;
    } else if (that == null) {
      return 1;
    }

    // Compare tuples of the same size only
    if (size() != that.size()) {
      throw new IAE("Cannot compare StringTuples of different sizes");
    }

    // Both tuples are empty
    if (size() == 0) {
      return 0;
    }

    // Compare the elements at each index until a differing element is found
    for (int i = 0; i < size(); ++i) {
      int comparison = nullSafeCompare(get(i), that.get(i));
      if (comparison != 0) {
        return comparison;
      }
    }

    return 0;
  }

  private int nullSafeCompare(String a, String b)
  {
    // Treat null as smaller than non-null
    if (a == null && b == null) {
      return 0;
    } else if (a == null) {
      return -1;
    } else if (b == null) {
      return 1;
    } else {
      return a.compareTo(b);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringTuple that = (StringTuple) o;
    return Arrays.equals(values, that.values);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(values);
  }

  @Override
  public String toString()
  {
    return Arrays.toString(values);
  }

  /**
   * Custom serializer that serializes a StringTuple as an array of String values.
   */
  static class Serializer extends StdSerializer<StringTuple>
  {
    private Serializer()
    {
      super(StringTuple.class);
    }

    @Override
    public void serialize(StringTuple value, JsonGenerator generator, SerializerProvider provider)
        throws IOException
    {
      generator.writeStartArray();
      for (int i = 0; i < value.size(); ++i) {
        generator.writeString(value.get(i));
      }
      generator.writeEndArray();
    }
  }

  /**
   * Custom deserializer that deserializes a StringTuple from an array of String values.
   */
  static class Deserializer extends StdDeserializer<StringTuple>
  {

    private Deserializer()
    {
      super(StringTuple.class);
    }

    @Override
    public StringTuple deserialize(JsonParser jsonParser, DeserializationContext context)
        throws IOException
    {
      final JsonNode jsonNode = jsonParser.getCodec().readTree(jsonParser);
      final String[] values = new String[jsonNode.size()];
      for (int i = 0; i < jsonNode.size(); ++i) {
        values[i] = jsonNode.get(i).asText();
      }
      return new StringTuple(values);
    }
  }
}
