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

package org.apache.druid.data.input.avro;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AvroLogicalTypeConverterTest
{

  private static final AvroLogicalTypeConverter UNDER_TEST = new AvroLogicalTypeConverter();

  private static final BigDecimal DECIMAL_AMOUNT = BigDecimal.valueOf(1234.12);
  private static final LocalDate DATE = LocalDate.of(2012, 12, 10);
  private static final Schema DECIMAL_SCHEMA = new Schema.Parser()
      .parse(
          "{\n"
          + "          \"type\": \"bytes\",\n"
          + "          \"logicalType\": \"decimal\",\n"
          + "          \"precision\": 6,\n"
          + "          \"scale\": 2\n"
          + "        }");
  private static final Schema DATE_SCHEMA = new Schema.Parser()
      .parse(
          "{\n"
          + "          \"type\": \"int\",\n"
          + "          \"logicalType\": \"date\"\n"
          + "        }");

  @Test
  public void convert_decimalType()
  {
    ByteBuffer bytes = new Conversions.DecimalConversion()
        .toBytes(
            DECIMAL_AMOUNT,
            DECIMAL_SCHEMA,
            LogicalTypes.fromSchema(DECIMAL_SCHEMA)
        );
    Object converted = UNDER_TEST.convert(bytes, DECIMAL_SCHEMA);

    assertEquals(1234.12, converted);
  }

  @Test
  public void convert_decimalType_whenTypeAlreadyLogical()
  {
    Object converted = UNDER_TEST.convert(DECIMAL_AMOUNT, DECIMAL_SCHEMA);

    assertEquals(1234.12, converted);
  }

  @Test
  public void convert_dateType()
  {
    Integer integerDate = new TimeConversions.DateConversion()
        .toInt(
            DATE,
            DATE_SCHEMA,
            LogicalTypes.fromSchema(DATE_SCHEMA)
        );
    Object converted = UNDER_TEST.convert(integerDate, DATE_SCHEMA);

    assertEquals(1355097600000L, converted);
  }

  @Test
  public void convert_dateType_whenTypeAlreadyLogical()
  {
    Object converted = UNDER_TEST.convert(DATE, DATE_SCHEMA);

    assertEquals(1355097600000L, converted);
  }

  @Test
  public void isRegistered_whenRegistered()
  {
    boolean returned = UNDER_TEST.isRegistered(LogicalTypes.fromSchema(DATE_SCHEMA));

    assertTrue(returned);
  }

  @Test
  public void isRegistered_whenNotRegistered()
  {
    boolean returned = UNDER_TEST.isRegistered(new UnmappedLogicalType("unmapped"));

    assertFalse(returned);
  }

  private static final class UnmappedLogicalType extends LogicalType
  {

    public UnmappedLogicalType(String logicalTypeName)
    {
      super(logicalTypeName);
    }
  }

}
