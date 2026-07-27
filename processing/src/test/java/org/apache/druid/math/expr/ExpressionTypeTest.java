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

package org.apache.druid.math.expr;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExpressionTypeTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ExpressionType SOME_COMPLEX = new ExpressionType(ExprType.COMPLEX, "foo", null);

  @Test
  public void testSerde() throws JsonProcessingException
  {
    Assertions.assertEquals(ExpressionType.STRING, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.STRING), ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.LONG), ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.DOUBLE), ExpressionType.class));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.STRING_ARRAY), ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.LONG_ARRAY), ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.DOUBLE_ARRAY), ExpressionType.class));
    Assertions.assertEquals(SOME_COMPLEX, MAPPER.readValue(MAPPER.writeValueAsString(SOME_COMPLEX), ExpressionType.class));
  }

  @Test
  public void testSerdeFromString() throws JsonProcessingException
  {
    Assertions.assertEquals(ExpressionType.STRING, MAPPER.readValue("\"STRING\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG, MAPPER.readValue("\"LONG\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue("\"DOUBLE\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"ARRAY<STRING>\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"ARRAY<LONG>\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<DOUBLE>\"", ExpressionType.class));

    ExpressionType whatHaveIdone = new ExpressionType(ExprType.ARRAY, null, new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX));
    Assertions.assertEquals(whatHaveIdone, MAPPER.readValue("\"ARRAY<ARRAY<COMPLEX<foo>>>\"", ExpressionType.class));

    Assertions.assertEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<foo>\"", ExpressionType.class));
    // make sure legacy works too
    Assertions.assertEquals(ExpressionType.STRING, MAPPER.readValue("\"string\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG, MAPPER.readValue("\"long\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue("\"double\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"STRING_ARRAY\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"LONG_ARRAY\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"DOUBLE_ARRAY\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"string_array\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"long_array\"", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"double_array\"", ExpressionType.class));
    // ARRAY<*> and COMPLEX<*> patterns must match exactly ...
    Assertions.assertNotEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"array<string>\"", ExpressionType.class));
    Assertions.assertNotEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"array<LONG>\"", ExpressionType.class));
    Assertions.assertNotEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<FOO>\"", ExpressionType.class));
    // this works though because array recursively calls on element type...
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<double>\"", ExpressionType.class));
  }

  @Test
  public void testFutureProof() throws JsonProcessingException
  {
    // in case we ever want to switch from string serde to JSON objects for type info, be ready
    Assertions.assertEquals(ExpressionType.STRING, MAPPER.readValue("{\"type\":\"STRING\"}", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG, MAPPER.readValue("{\"type\":\"LONG\"}", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue("{\"type\":\"DOUBLE\"}", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"STRING\"}}", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"LONG\"}}", ExpressionType.class));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"DOUBLE\"}}", ExpressionType.class));

    Assertions.assertEquals(SOME_COMPLEX, MAPPER.readValue("{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}", ExpressionType.class));

    ExpressionType whatHaveIdone = new ExpressionType(ExprType.ARRAY, null, new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX));
    Assertions.assertEquals(whatHaveIdone, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}}}", ExpressionType.class));
  }

  @Test
  public void testConvertFromColumnType()
  {
    Assertions.assertNull(ExpressionType.fromColumnType(null));
    Assertions.assertEquals(ExpressionType.LONG, ExpressionType.fromColumnType(ColumnType.LONG));
    Assertions.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnType(ColumnType.FLOAT));
    Assertions.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnType(ColumnType.DOUBLE));
    Assertions.assertEquals(ExpressionType.STRING, ExpressionType.fromColumnType(ColumnType.STRING));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, ExpressionType.fromColumnType(ColumnType.LONG_ARRAY));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, ExpressionType.fromColumnType(ColumnType.DOUBLE_ARRAY));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, ExpressionType.fromColumnType(ColumnType.STRING_ARRAY));
    Assertions.assertEquals(
        SOME_COMPLEX,
        ExpressionType.fromColumnType(ColumnType.ofComplex(SOME_COMPLEX.getComplexTypeName()))
    );
    ExpressionType complexArray = new ExpressionType(
        ExprType.ARRAY,
        null,
        new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX)
    );
    ColumnType complexArrayColumn = new ColumnType(
        ValueType.ARRAY,
        null,
        new ColumnType(ValueType.ARRAY,
                       null,
                       ColumnType.ofComplex(SOME_COMPLEX.getComplexTypeName())
        )
    );
    Assertions.assertEquals(complexArray, ExpressionType.fromColumnType(complexArrayColumn));
  }

  @Test
  public void testConvertFromColumnTypeStrict()
  {
    Assertions.assertEquals(ExpressionType.LONG, ExpressionType.fromColumnTypeStrict(ColumnType.LONG));
    Assertions.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnTypeStrict(ColumnType.FLOAT));
    Assertions.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnTypeStrict(ColumnType.DOUBLE));
    Assertions.assertEquals(ExpressionType.STRING, ExpressionType.fromColumnTypeStrict(ColumnType.STRING));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, ExpressionType.fromColumnTypeStrict(ColumnType.LONG_ARRAY));
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, ExpressionType.fromColumnTypeStrict(ColumnType.DOUBLE_ARRAY));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, ExpressionType.fromColumnTypeStrict(ColumnType.STRING_ARRAY));
    Assertions.assertEquals(
        SOME_COMPLEX,
        ExpressionType.fromColumnTypeStrict(ColumnType.ofComplex(SOME_COMPLEX.getComplexTypeName()))
    );
    ExpressionType complexArray = new ExpressionType(
        ExprType.ARRAY,
        null,
        new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX)
    );
    ColumnType complexArrayColumn = new ColumnType(
        ValueType.ARRAY,
        null,
        new ColumnType(ValueType.ARRAY,
                       null,
                       ColumnType.ofComplex(SOME_COMPLEX.getComplexTypeName())
        )
    );
    Assertions.assertEquals(complexArray, ExpressionType.fromColumnTypeStrict(complexArrayColumn));
  }

  @Test
  public void testConvertFromColumnTypeStrictNull()
  {
    IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> ExpressionType.fromColumnTypeStrict(null)
    );
    Assertions.assertTrue(e.getMessage().contains("Unsupported unknown value type"));
  }

  @Test
  public void testConvertToColumnType()
  {
    Assertions.assertEquals(ColumnType.LONG, ExpressionType.toColumnType(ExpressionType.LONG));
    Assertions.assertEquals(ColumnType.DOUBLE, ExpressionType.toColumnType(ExpressionType.DOUBLE));
    Assertions.assertEquals(ColumnType.STRING, ExpressionType.toColumnType(ExpressionType.STRING));
    Assertions.assertEquals(ColumnType.LONG_ARRAY, ExpressionType.toColumnType(ExpressionType.LONG_ARRAY));
    Assertions.assertEquals(ColumnType.DOUBLE_ARRAY, ExpressionType.toColumnType(ExpressionType.DOUBLE_ARRAY));
    Assertions.assertEquals(ColumnType.STRING_ARRAY, ExpressionType.toColumnType(ExpressionType.STRING_ARRAY));
    Assertions.assertEquals(
        ColumnType.ofComplex(SOME_COMPLEX.getComplexTypeName()),
        ExpressionType.toColumnType(SOME_COMPLEX)
    );
    ExpressionType complexArray = new ExpressionType(
        ExprType.ARRAY,
        null,
        new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX)
    );
    ColumnType complexArrayColumn = new ColumnType(
        ValueType.ARRAY,
        null,
        new ColumnType(ValueType.ARRAY,
                       null,
                       ColumnType.ofComplex(SOME_COMPLEX.getComplexTypeName())
        )
    );
    Assertions.assertEquals(complexArrayColumn, ExpressionType.toColumnType(complexArray));
  }
}
