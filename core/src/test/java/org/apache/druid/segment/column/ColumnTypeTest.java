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
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

public class ColumnTypeTest
{
  public static final ColumnType SOME_COMPLEX = new ColumnType(ValueType.COMPLEX, "foo", null);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    Assert.assertEquals(ColumnType.STRING, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.STRING), ColumnType.class));
    Assert.assertEquals(ColumnType.LONG, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.LONG), ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.DOUBLE), ColumnType.class));
    Assert.assertEquals(ColumnType.FLOAT, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.FLOAT), ColumnType.class));
    Assert.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.STRING_ARRAY), ColumnType.class));
    Assert.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.LONG_ARRAY), ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.DOUBLE_ARRAY), ColumnType.class));
    Assert.assertEquals(ColumnType.UNKNOWN_COMPLEX, MAPPER.readValue(MAPPER.writeValueAsString(ColumnType.UNKNOWN_COMPLEX), ColumnType.class));
    Assert.assertEquals(SOME_COMPLEX, MAPPER.readValue(MAPPER.writeValueAsString(SOME_COMPLEX), ColumnType.class));
  }

  @Test
  public void testSerdeLegacy() throws JsonProcessingException
  {
    Assert.assertEquals(ColumnType.STRING, MAPPER.readValue("\"STRING\"", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG, MAPPER.readValue("\"LONG\"", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE, MAPPER.readValue("\"DOUBLE\"", ColumnType.class));
    Assert.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"ARRAY<STRING>\"", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"ARRAY<LONG>\"", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<DOUBLE>\"", ColumnType.class));

    ColumnType whatHaveIdone = new ColumnType(ValueType.ARRAY, null, new ColumnType(ValueType.ARRAY, null, SOME_COMPLEX));
    Assert.assertEquals(whatHaveIdone, MAPPER.readValue("\"ARRAY<ARRAY<COMPLEX<foo>>>\"", ColumnType.class));

    Assert.assertEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<foo>\"", ColumnType.class));
    // make sure legacy works too
    Assert.assertEquals(ColumnType.STRING, MAPPER.readValue("\"string\"", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG, MAPPER.readValue("\"long\"", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE, MAPPER.readValue("\"double\"", ColumnType.class));
    Assert.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"STRING_ARRAY\"", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"LONG_ARRAY\"", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"DOUBLE_ARRAY\"", ColumnType.class));
    Assert.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"string_array\"", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"long_array\"", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"double_array\"", ColumnType.class));
    // ARRAY<*> and COMPLEX<*> patterns must match exactly ...
    Assert.assertNotEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("\"array<string>\"", ColumnType.class));
    Assert.assertNotEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("\"array<LONG>\"", ColumnType.class));
    Assert.assertNotEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<FOO>\"", ColumnType.class));
    // this works though because array recursively calls on element type...
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<double>\"", ColumnType.class));
  }

  @Test
  public void testFutureProof() throws JsonProcessingException
  {
    // in case we ever want to switch from string serde to JSON objects for type info, be ready
    Assert.assertEquals(ColumnType.STRING, MAPPER.readValue("{\"type\":\"STRING\"}", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG, MAPPER.readValue("{\"type\":\"LONG\"}", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE, MAPPER.readValue("{\"type\":\"DOUBLE\"}", ColumnType.class));
    Assert.assertEquals(ColumnType.STRING_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"STRING\"}}", ColumnType.class));
    Assert.assertEquals(ColumnType.LONG_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"LONG\"}}", ColumnType.class));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"DOUBLE\"}}", ColumnType.class));

    Assert.assertEquals(SOME_COMPLEX, MAPPER.readValue("{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}", ColumnType.class));

    ColumnType whatHaveIdone = new ColumnType(ValueType.ARRAY, null, new ColumnType(ValueType.ARRAY, null, SOME_COMPLEX));
    Assert.assertEquals(whatHaveIdone, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}}}", ColumnType.class));
  }

  @Test
  public void testFactoryFromOtherTypeSignatures()
  {
    Assert.assertEquals(ColumnType.LONG, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.LONG, null, null)));
    Assert.assertEquals(ColumnType.LONG, ColumnTypeFactory.ofValueType(ValueType.LONG));
    Assert.assertEquals(ColumnType.FLOAT, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.FLOAT, null, null)));
    Assert.assertEquals(ColumnType.FLOAT, ColumnTypeFactory.ofValueType(ValueType.FLOAT));
    Assert.assertEquals(ColumnType.DOUBLE, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.DOUBLE, null, null)));
    Assert.assertEquals(ColumnType.DOUBLE, ColumnTypeFactory.ofValueType(ValueType.DOUBLE));
    Assert.assertEquals(ColumnType.STRING, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.STRING, null, null)));
    Assert.assertEquals(ColumnType.STRING, ColumnTypeFactory.ofValueType(ValueType.STRING));
    Assert.assertEquals(
        ColumnType.LONG_ARRAY,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(
                ValueType.ARRAY,
                null,
                new SomeOtherTypeSignature(ValueType.LONG, null, null)
            )
        )
    );
    Assert.assertEquals(
        ColumnType.DOUBLE_ARRAY,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(
                ValueType.ARRAY,
                null,
                new SomeOtherTypeSignature(ValueType.DOUBLE, null, null)
            )
        )
    );
    Assert.assertEquals(
        ColumnType.STRING_ARRAY,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(
                ValueType.ARRAY,
                null,
                new SomeOtherTypeSignature(ValueType.STRING, null, null)
            )
        )
    );
    Assert.assertEquals(ColumnType.UNKNOWN_COMPLEX, ColumnTypeFactory.ofType(new SomeOtherTypeSignature(ValueType.COMPLEX, null, null)));
    Assert.assertEquals(ColumnType.UNKNOWN_COMPLEX, ColumnTypeFactory.ofValueType(ValueType.COMPLEX));
    Assert.assertEquals(
        SOME_COMPLEX,
        ColumnTypeFactory.ofType(
            new SomeOtherTypeSignature(ValueType.COMPLEX, SOME_COMPLEX.getComplexTypeName(), null)
        )
    );
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
