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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

public class ColumnTypeTest
{
  public static final ColumnType SOME_COMPLEX = new ColumnType(ValueType.COMPLEX, "foo", null);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    Assertions.assertEquals(ColumnType.STRING, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.STRING), ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.LONG), ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.DOUBLE), ColumnType.class));
    Assertions.assertEquals(ColumnType.FLOAT, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.FLOAT), ColumnType.class));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.STRING_ARRAY), ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.LONG_ARRAY), ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.DOUBLE_ARRAY), ColumnType.class));
    Assertions.assertEquals(ColumnType.UNKNOWN_COMPLEX, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.UNKNOWN_COMPLEX), ColumnType.class));
    Assertions.assertEquals(SOME_COMPLEX, MAPPER.readValue(MAPPER.writeValueAsString(SOME_COMPLEX), ColumnType.class));
  }

  @Test
  public void testSerdeLegacy() throws JsonProcessingException
  {
    Assertions.assertEquals(ColumnType.STRING, MAPPER.readValue("\"STRING\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG, MAPPER.readValue("\"LONG\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE, MAPPER.readValue("\"DOUBLE\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"ARRAY<STRING>\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"ARRAY<LONG>\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<DOUBLE>\"", ColumnType.class));

    ColumnType whatHaveIdone = new ColumnType(ValueType.ARRAY, null, new ColumnType(ValueType.ARRAY, null, SOME_COMPLEX));
    Assertions.assertEquals(whatHaveIdone, MAPPER.readValue("\"ARRAY<ARRAY<COMPLEX<foo>>>\"", ColumnType.class));

    Assertions.assertEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<foo>\"", ColumnType.class));
    // make sure legacy works too
    Assertions.assertEquals(ColumnType.STRING, MAPPER.readValue("\"string\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG, MAPPER.readValue("\"long\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE, MAPPER.readValue("\"double\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"STRING_ARRAY\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"LONG_ARRAY\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"DOUBLE_ARRAY\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"string_array\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"long_array\"", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"double_array\"", ColumnType.class));
    // ARRAY<*> and COMPLEX<*> patterns must match exactly ...
    Assertions.assertNotEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"array<string>\"", ColumnType.class));
    Assertions.assertNotEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"array<LONG>\"", ColumnType.class));
    Assertions.assertNotEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<FOO>\"", ColumnType.class));
    // this works though because array recursively calls on element type...
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<double>\"", ColumnType.class));
  }

  @Test
  public void testFutureProof() throws JsonProcessingException
  {
    // in case we ever want to switch from string serde to JSON objects for type info, be ready
    Assertions.assertEquals(ColumnType.STRING, MAPPER.readValue("{\"type\":\"STRING\"}", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG, MAPPER.readValue("{\"type\":\"LONG\"}", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE, MAPPER.readValue("{\"type\":\"DOUBLE\"}", ColumnType.class));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"STRING\"}}", ColumnType.class));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"LONG\"}}", ColumnType.class));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"DOUBLE\"}}", ColumnType.class));

    Assertions.assertEquals(SOME_COMPLEX, MAPPER.readValue("{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}", ColumnType.class));

    ColumnType whatHaveIdone = new ColumnType(ValueType.ARRAY, null, new ColumnType(ValueType.ARRAY, null, SOME_COMPLEX));
    Assertions.assertEquals(whatHaveIdone, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}}}", ColumnType.class));
  }

  @Test
  public void testFactoryFromOtherTypeSignatures()
  {
    Assertions.assertEquals(ColumnType.LONG, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.LONG, null, null)));
    Assertions.assertEquals(ColumnType.LONG, ColumnTypeFactory.ofValueType(ValueType.LONG));
    Assertions.assertEquals(ColumnType.FLOAT, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.FLOAT, null, null)));
    Assertions.assertEquals(ColumnType.FLOAT, ColumnTypeFactory.ofValueType(ValueType.FLOAT));
    Assertions.assertEquals(ColumnType.DOUBLE, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.DOUBLE, null, null)));
    Assertions.assertEquals(ColumnType.DOUBLE, ColumnTypeFactory.ofValueType(ValueType.DOUBLE));
    Assertions.assertEquals(ColumnType.STRING, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.STRING, null, null)));
    Assertions.assertEquals(ColumnType.STRING, ColumnTypeFactory.ofValueType(ValueType.STRING));
    Assertions.assertEquals(
        ColumnType.LONG_ARRAY,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(
                ValueType.ARRAY,
                null,
                new SomeOtherTypeSignature(ValueType.LONG, null, null)
            )
        )
    );
    Assertions.assertEquals(
        ColumnType.DOUBLE_ARRAY,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(
                ValueType.ARRAY,
                null,
                new SomeOtherTypeSignature(ValueType.DOUBLE, null, null)
            )
        )
    );
    Assertions.assertEquals(
        ColumnType.STRING_ARRAY,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(
                ValueType.ARRAY,
                null,
                new SomeOtherTypeSignature(ValueType.STRING, null, null)
            )
        )
    );
    Assertions.assertEquals(ColumnType.UNKNOWN_COMPLEX, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.COMPLEX, null, null)));
    Assertions.assertEquals(ColumnType.UNKNOWN_COMPLEX, ColumnTypeFactory.ofValueType(ValueType.COMPLEX));
    Assertions.assertEquals(
        SOME_COMPLEX,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(ValueType.COMPLEX, SOME_COMPLEX.getComplexTypeName(), null)
        )
    );
  }

  @Test
  public void testLeastRestrictiveType()
  {
    Assertions.assertEquals(ColumnType.STRING, ColumnType.leastRestrictiveType(ColumnType.STRING, ColumnType.DOUBLE));
    Assertions.assertEquals(ColumnType.STRING, ColumnType.leastRestrictiveType(ColumnType.STRING, ColumnType.LONG));
    Assertions.assertEquals(ColumnType.STRING, ColumnType.leastRestrictiveType(ColumnType.STRING, ColumnType.FLOAT));
    Assertions.assertEquals(ColumnType.DOUBLE, ColumnType.leastRestrictiveType(ColumnType.FLOAT, ColumnType.DOUBLE));
    Assertions.assertEquals(ColumnType.DOUBLE, ColumnType.leastRestrictiveType(ColumnType.FLOAT, ColumnType.LONG));
    Assertions.assertEquals(ColumnType.LONG, ColumnType.leastRestrictiveType(ColumnType.LONG, ColumnType.LONG));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, ColumnType.leastRestrictiveType(ColumnType.LONG, ColumnType.LONG_ARRAY));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, ColumnType.leastRestrictiveType(ColumnType.STRING_ARRAY, ColumnType.LONG_ARRAY));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, ColumnType.leastRestrictiveType(ColumnType.STRING_ARRAY, ColumnType.DOUBLE_ARRAY));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, ColumnType.leastRestrictiveType(ColumnType.LONG_ARRAY, ColumnType.DOUBLE_ARRAY));
    Assertions.assertEquals(ColumnType.NESTED_DATA, ColumnType.leastRestrictiveType(ColumnType.STRING_ARRAY, ColumnType.NESTED_DATA));
    Assertions.assertEquals(ColumnType.NESTED_DATA, ColumnType.leastRestrictiveType(ColumnType.NESTED_DATA, ColumnType.STRING_ARRAY));
    Assertions.assertEquals(ColumnType.NESTED_DATA, ColumnType.leastRestrictiveType(ColumnType.NESTED_DATA, ColumnType.UNKNOWN_COMPLEX));
    Assertions.assertEquals(SOME_COMPLEX, ColumnType.leastRestrictiveType(SOME_COMPLEX, ColumnType.UNKNOWN_COMPLEX));
    Assertions.assertEquals(SOME_COMPLEX, ColumnType.leastRestrictiveType(ColumnType.UNKNOWN_COMPLEX, SOME_COMPLEX));
    Assertions.assertThrows(Types.IncompatibleTypeException.class, () -> ColumnType.leastRestrictiveType(ColumnType.NESTED_DATA, SOME_COMPLEX));
    Assertions.assertThrows(Types.IncompatibleTypeException.class, () -> ColumnType.leastRestrictiveType(ColumnType.STRING_ARRAY, SOME_COMPLEX));
  }

  static class SomeOtherTypeSignature extends BaseTypeSignature<ValueType>
  {
    public SomeOtherTypeSignature(
        ValueType valueType,
        @Nullable String complexTypeName,
        @Nullable TypeSignature<ValueType> elementType
    )
    {
      super(ColumnTypeFactory.getInstance(), valueType, complexTypeName, elementType);
    }

    @Override
    public <T> TypeStrategy<T> getStrategy()
    {
      throw new UnsupportedOperationException("nope");
    }
  }
}
