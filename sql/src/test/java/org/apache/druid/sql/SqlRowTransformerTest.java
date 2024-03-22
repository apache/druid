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

package org.apache.druid.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SqlRowTransformerTest extends CalciteTestBase
{
  private RelDataType rowType;

  @BeforeEach
  public void setup()
  {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);
    rowType = typeFactory.createStructType(
        ImmutableList.of(
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
            typeFactory.createSqlType(SqlTypeName.DATE),
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR)
        ),
        ImmutableList.of(
            "timestamp_col",
            "date_col",
            "string_col",
            "null"
        )
    );
  }

  @Test
  public void testTransformUTC()
  {
    SqlRowTransformer transformer = new SqlRowTransformer(
        DateTimeZone.UTC,
        rowType
    );
    DateTime timestamp = DateTimes.of("2021-08-01T12:00:00");
    DateTime date = DateTimes.of("2021-01-01");
    Object[] expectedRow = new Object[]{
        ISODateTimeFormat.dateTime().print(timestamp),
        ISODateTimeFormat.dateTime().print(date),
        "string",
        null
    };
    Object[] row = new Object[]{
        Calcites.jodaToCalciteTimestamp(timestamp, DateTimeZone.UTC),
        Calcites.jodaToCalciteDate(date, DateTimeZone.UTC),
        expectedRow[2],
        null
    };
    Assert.assertArrayEquals(
        expectedRow,
        IntStream.range(0, expectedRow.length).mapToObj(i -> transformer.transform(row, i)).toArray()
    );
  }

  @Test
  public void testTransformNonUTC()
  {
    DateTimeZone timeZone = DateTimes.inferTzFromString("Asia/Seoul");
    SqlRowTransformer transformer = new SqlRowTransformer(
        timeZone,
        rowType
    );
    DateTime timestamp = new DateTime("2021-08-01T12:00:00", timeZone);
    DateTime date = new DateTime("2021-01-01", timeZone);
    Object[] expectedRow = new Object[]{
        ISODateTimeFormat.dateTime().withZone(timeZone).print(timestamp),
        ISODateTimeFormat.dateTime().withZone(timeZone).print(date),
        "string",
        null
    };
    Object[] row = new Object[]{
        Calcites.jodaToCalciteTimestamp(timestamp, timeZone),
        Calcites.jodaToCalciteDate(date, timeZone),
        expectedRow[2],
        null
    };
    Assert.assertArrayEquals(
        expectedRow,
        IntStream.range(0, expectedRow.length).mapToObj(i -> transformer.transform(row, i)).toArray()
    );
  }

  @Test
  public void testGetFieldList()
  {
    SqlRowTransformer transformer = new SqlRowTransformer(
        DateTimeZone.UTC,
        rowType
    );

    Assert.assertEquals(
        rowType.getFieldList().stream().map(RelDataTypeField::getName).collect(Collectors.toList()),
        transformer.getFieldList()
    );
  }
}
