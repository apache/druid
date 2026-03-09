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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ThriftInputFormatTest
{
  private static final String THRIFT_CLASS = "org.apache.druid.data.input.thrift.Book";

  private Book book;
  private InputRowSchema schema;
  private JSONPathSpec flattenSpec;

  @Before
  public void setUp()
  {
    book = new Book()
        .setDate("2016-08-29")
        .setPrice(19.9)
        .setTitle("title")
        .setAuthor(new Author().setFirstName("first").setLastName("last"));

    schema = new InputRowSchema(
        new TimestampSpec("date", "auto", null),
        new DimensionsSpec(Lists.newArrayList(
            new StringDimensionSchema("title"),
            new StringDimensionSchema("lastName")
        )),
        ColumnsFilter.all()
    );

    flattenSpec = new JSONPathSpec(
        true,
        Lists.newArrayList(
            new JSONPathFieldSpec(JSONPathFieldType.ROOT, "title", "title"),
            new JSONPathFieldSpec(JSONPathFieldType.PATH, "lastName", "$.author.lastName")
        )
    );
  }

  @Test
  public void testParseCompact() throws Exception
  {
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    byte[] bytes = serializer.serialize(book);
    assertParsedRow(bytes);
  }

  @Test
  public void testParseBinaryBase64() throws Exception
  {
    TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
    byte[] bytes = StringUtils.encodeBase64(serializer.serialize(book));
    assertParsedRow(bytes);
  }

  @Test
  public void testParseJson() throws Exception
  {
    TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
    byte[] bytes = serializer.serialize(book);
    assertParsedRow(bytes);
  }

  @Test
  public void testParseWithJarPath() throws Exception
  {
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    byte[] bytes = serializer.serialize(book);

    ThriftInputFormat format = new ThriftInputFormat(flattenSpec, "example/book.jar", THRIFT_CLASS);
    InputRow row = readSingleRow(format, bytes);
    Assert.assertEquals("title", row.getDimension("title").get(0));
    Assert.assertEquals("last", row.getDimension("lastName").get(0));
  }

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    for (Module module : new ThriftExtensionsModule().getJacksonModules()) {
      mapper.registerModule(module);
    }

    ThriftInputFormat format = new ThriftInputFormat(flattenSpec, null, THRIFT_CLASS);
    String json = mapper.writeValueAsString(format);
    ThriftInputFormat deserialized = (ThriftInputFormat) mapper.readValue(json, org.apache.druid.data.input.InputFormat.class);

    Assert.assertEquals(format, deserialized);
  }

  @Test
  public void testIsSplittable()
  {
    ThriftInputFormat format = new ThriftInputFormat(null, null, THRIFT_CLASS);
    Assert.assertFalse(format.isSplittable());
  }

  @Test
  public void testEquals()
  {
    ThriftInputFormat format1 = new ThriftInputFormat(flattenSpec, null, THRIFT_CLASS);
    ThriftInputFormat format2 = new ThriftInputFormat(flattenSpec, null, THRIFT_CLASS);
    ThriftInputFormat format3 = new ThriftInputFormat(null, null, THRIFT_CLASS);

    Assert.assertEquals(format1, format2);
    Assert.assertNotEquals(format1, format3);
  }

  private void assertParsedRow(byte[] bytes) throws IOException
  {
    ThriftInputFormat format = new ThriftInputFormat(flattenSpec, null, THRIFT_CLASS);
    InputRow row = readSingleRow(format, bytes);
    Assert.assertEquals("title", row.getDimension("title").get(0));
    Assert.assertEquals("last", row.getDimension("lastName").get(0));
  }

  private InputRow readSingleRow(ThriftInputFormat format, byte[] bytes) throws IOException
  {
    InputEntityReader reader = format.createReader(schema, new ByteEntity(bytes), null);
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      Assert.assertTrue(iterator.hasNext());
      InputRow row = iterator.next();
      Assert.assertFalse(iterator.hasNext());
      return row;
    }
  }
}
