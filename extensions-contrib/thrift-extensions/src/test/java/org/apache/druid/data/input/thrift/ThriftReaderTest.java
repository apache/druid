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

package org.apache.druid.data.input.thrift;

import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;

public class ThriftReaderTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private InputRowSchema inputRowSchema;
  private InputRowSchema flattenInputRowSchema;
  private JSONPathSpec flattenSpec;
  private DateTime dateTime;
  private String date;

  @Before
  public void setUp()
  {
    TimestampSpec timestampSpec = new TimestampSpec("date", "auto", null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(Lists.newArrayList(
        new StringDimensionSchema("title"),
        new StringDimensionSchema("lastName")
    ), null, null);
    DimensionsSpec flattenDimensionsSpec = new DimensionsSpec(Collections.singletonList(
        new StringDimensionSchema("title")
    ), null, null);
    flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "title", "title"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "lastName", "$.author.lastName")
        )
    );

    inputRowSchema = new InputRowSchema(timestampSpec, dimensionsSpec, null);
    flattenInputRowSchema = new InputRowSchema(timestampSpec, flattenDimensionsSpec, null);
    dateTime = new DateTime(2016, 8, 29, 0, 0, ISOChronology.getInstanceUTC());
    date = "2016-08-29";
  }

  @Test
  public void testParseNestedData() throws Exception
  {
    Book book = new Book().setDate(date).setPrice(19.9).setTitle("title")
        .setAuthor(new Author().setFirstName("first").setLastName("last"));
    TSerializer serializer;
    byte[] bytes;
    serializer = new TSerializer(new TJSONProtocol.Factory());
    bytes = serializer.serialize(book);
    final ByteEntity entity = new ByteEntity(bytes);

    ThriftReader reader = new ThriftReader(inputRowSchema, entity, "example/book.jar", "org.apache.druid.data.input.thrift.Book", flattenSpec);
    InputRow row = reader.read().next();

    verifyNestedData(row, this.dateTime);
  }

  @Test
  public void testParseFlatData() throws Exception
  {
    Book book = new Book().setDate(date).setPrice(19.9).setTitle("title");
    TSerializer serializer;
    byte[] bytes;
    serializer = new TSerializer(new TJSONProtocol.Factory());
    bytes = serializer.serialize(book);
    final ByteEntity entity = new ByteEntity(bytes);

    ThriftReader reader = new ThriftReader(flattenInputRowSchema, entity, "example/book.jar", "org.apache.druid.data.input.thrift.Book", JSONPathSpec.DEFAULT);
    InputRow row = reader.read().next();

    verifyFlattenData(row, this.dateTime);
  }


  static void verifyNestedData(InputRow row, DateTime dateTime)
  {
    Assert.assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "title", "title");
    assertDimensionEquals(row, "lastName", "last");

    Assert.assertEquals(19.9F, row.getMetric("price").floatValue(), 0.0);
  }

  static void verifyFlattenData(InputRow row, DateTime dateTime)
  {
    Assert.assertEquals(dateTime.getMillis(), row.getTimestampFromEpoch());

    assertDimensionEquals(row, "title", "title");

    Assert.assertEquals(19.9F, row.getMetric("price").floatValue(), 0.0);
  }

  private static void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }

}
