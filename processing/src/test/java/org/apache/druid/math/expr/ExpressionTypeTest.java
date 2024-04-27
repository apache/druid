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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExpressionTypeTest
{
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ExpressionType SOME_COMPLEX = new ExpressionType(ExprType.COMPLEX, "foo", null);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws JsonProcessingException
  {
    Assert.assertEquals(ExpressionType.STRING, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.STRING), ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.LONG), ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.DOUBLE), ExpressionType.class));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.STRING_ARRAY), ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.LONG_ARRAY), ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue(MAPPER.writeValueAsString(ExpressionType.DOUBLE_ARRAY), ExpressionType.class));
    Assert.assertEquals(SOME_COMPLEX, MAPPER.readValue(MAPPER.writeValueAsString(SOME_COMPLEX), ExpressionType.class));
  }

  @Test
  public void testSerdeFromString() throws JsonProcessingException
  {
    Assert.assertEquals(ExpressionType.STRING, MAPPER.readValue("\"STRING\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG, MAPPER.readValue("\"LONG\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue("\"DOUBLE\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"ARRAY<STRING>\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"ARRAY<LONG>\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<DOUBLE>\"", ExpressionType.class));

    ExpressionType whatHaveIdone = new ExpressionType(ExprType.ARRAY, null, new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX));
    Assert.assertEquals(whatHaveIdone, MAPPER.readValue("\"ARRAY<ARRAY<COMPLEX<foo>>>\"", ExpressionType.class));

    Assert.assertEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<foo>\"", ExpressionType.class));
    // make sure legacy works too
    Assert.assertEquals(ExpressionType.STRING, MAPPER.readValue("\"string\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG, MAPPER.readValue("\"long\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue("\"double\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"STRING_ARRAY\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"LONG_ARRAY\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"DOUBLE_ARRAY\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"string_array\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"long_array\"", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"double_array\"", ExpressionType.class));
    // ARRAY<*> and COMPLEX<*> patterns must match exactly ...
    Assert.assertNotEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("\"array<string>\"", ExpressionType.class));
    Assert.assertNotEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("\"array<LONG>\"", ExpressionType.class));
    Assert.assertNotEquals(SOME_COMPLEX, MAPPER.readValue("\"COMPLEX<FOO>\"", ExpressionType.class));
    // this works though because array recursively calls on element type...
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("\"ARRAY<double>\"", ExpressionType.class));
  }

  @Test
  public void testFutureProof() throws JsonProcessingException
  {
    // in case we ever want to switch from string serde to JSON objects for type info, be ready
    Assert.assertEquals(ExpressionType.STRING, MAPPER.readValue("{\"type\":\"STRING\"}", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG, MAPPER.readValue("{\"type\":\"LONG\"}", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE, MAPPER.readValue("{\"type\":\"DOUBLE\"}", ExpressionType.class));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"STRING\"}}", ExpressionType.class));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"LONG\"}}", ExpressionType.class));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"DOUBLE\"}}", ExpressionType.class));

    Assert.assertEquals(SOME_COMPLEX, MAPPER.readValue("{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}", ExpressionType.class));

    ExpressionType whatHaveIdone = new ExpressionType(ExprType.ARRAY, null, new ExpressionType(ExprType.ARRAY, null, SOME_COMPLEX));
    Assert.assertEquals(whatHaveIdone, MAPPER.readValue("{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"ARRAY\", \"elementType\":{\"type\":\"COMPLEX\", \"complexTypeName\":\"foo\"}}}", ExpressionType.class));
  }

  @Test
  public void testConvertFromColumnType()
  {
    Assert.assertNull(ExpressionType.fromColumnType(null));
    Assert.assertEquals(ExpressionType.LONG, ExpressionType.fromColumnType(ColumnType.LONG));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnType(ColumnType.FLOAT));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnType(ColumnType.DOUBLE));
    Assert.assertEquals(ExpressionType.STRING, ExpressionType.fromColumnType(ColumnType.STRING));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, ExpressionType.fromColumnType(ColumnType.LONG_ARRAY));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, ExpressionType.fromColumnType(ColumnType.DOUBLE_ARRAY));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, ExpressionType.fromColumnType(ColumnType.STRING_ARRAY));
    Assert.assertEquals(
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
    Assert.assertEquals(complexArray, ExpressionType.fromColumnType(complexArrayColumn));
  }

  @Test
  public void testConvertFromColumnTypeStrict()
  {
    Assert.assertEquals(ExpressionType.LONG, ExpressionType.fromColumnTypeStrict(ColumnType.LONG));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnTypeStrict(ColumnType.FLOAT));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionType.fromColumnTypeStrict(ColumnType.DOUBLE));
    Assert.assertEquals(ExpressionType.STRING, ExpressionType.fromColumnTypeStrict(ColumnType.STRING));
    Assert.assertEquals(ExpressionType.LONG_ARRAY, ExpressionType.fromColumnTypeStrict(ColumnType.LONG_ARRAY));
    Assert.assertEquals(ExpressionType.DOUBLE_ARRAY, ExpressionType.fromColumnTypeStrict(ColumnType.DOUBLE_ARRAY));
    Assert.assertEquals(ExpressionType.STRING_ARRAY, ExpressionType.fromColumnTypeStrict(ColumnType.STRING_ARRAY));
    Assert.assertEquals(
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
    Assert.assertEquals(complexArray, ExpressionType.fromColumnTypeStrict(complexArrayColumn));
  }

  @Test
  public void testConvertFromColumnTypeStrictNull()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Unsupported unknown value type");
    ExpressionType.fromColumnTypeStrict(null);
  }

  @Test
  public void testConvertToColumnType()
  {
    Assert.assertEquals(ColumnType.LONG, ExpressionType.toColumnType(ExpressionType.LONG));
    Assert.assertEquals(ColumnType.DOUBLE, ExpressionType.toColumnType(ExpressionType.DOUBLE));
    Assert.assertEquals(ColumnType.STRING, ExpressionType.toColumnType(ExpressionType.STRING));
    Assert.assertEquals(ColumnType.LONG_ARRAY, ExpressionType.toColumnType(ExpressionType.LONG_ARRAY));
    Assert.assertEquals(ColumnType.DOUBLE_ARRAY, ExpressionType.toColumnType(ExpressionType.DOUBLE_ARRAY));
    Assert.assertEquals(ColumnType.STRING_ARRAY, ExpressionType.toColumnType(ExpressionType.STRING_ARRAY));
    Assert.assertEquals(
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
    Assert.assertEquals(complexArrayColumn, ExpressionType.toColumnType(complexArray));
  }
}
