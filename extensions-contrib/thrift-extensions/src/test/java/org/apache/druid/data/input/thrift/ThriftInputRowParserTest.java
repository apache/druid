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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.JavaScriptParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

public class ThriftInputRowParserTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ParseSpec parseSpec;

  @Before
  public void setUp()
  {
    parseSpec = new JSONParseSpec(new TimestampSpec("date", "auto", null),
                                  new DimensionsSpec(Lists.newArrayList(
                                      new StringDimensionSchema("title"),
                                      new StringDimensionSchema("lastName")
                                  ), null, null),
                                  new JSONPathSpec(
                                      true,
                                      Lists.newArrayList(
                                          new JSONPathFieldSpec(JSONPathFieldType.ROOT, "title", "title"),
                                          new JSONPathFieldSpec(JSONPathFieldType.PATH, "lastName", "$.author.lastName")
                                      )
                                  ),
                                  null,
                                  null
    );
  }

  @Test
  public void testGetThriftClass() throws Exception
  {
    ThriftInputRowParser parser1 = new ThriftInputRowParser(
        parseSpec,
        "example/book.jar",
        "org.apache.druid.data.input.thrift.Book"
    );
    Assert.assertEquals("org.apache.druid.data.input.thrift.Book", parser1.getThriftClass().getName());

    ThriftInputRowParser parser2 = new ThriftInputRowParser(parseSpec, null, "org.apache.druid.data.input.thrift.Book");
    Assert.assertEquals("org.apache.druid.data.input.thrift.Book", parser2.getThriftClass().getName());
  }

  @Test
  public void testParse() throws Exception
  {
    ThriftInputRowParser parser = new ThriftInputRowParser(
        parseSpec,
        "example/book.jar",
        "org.apache.druid.data.input.thrift.Book"
    );
    Book book = new Book().setDate("2016-08-29").setPrice(19.9).setTitle("title")
                          .setAuthor(new Author().setFirstName("first").setLastName("last"));

    TSerializer serializer;
    byte[] bytes;

    // 1. compact
    serializer = new TSerializer(new TCompactProtocol.Factory());
    bytes = serializer.serialize(book);
    serializationAndTest(parser, bytes);

    // 2. binary + base64
    serializer = new TSerializer(new TBinaryProtocol.Factory());
    serializationAndTest(parser, StringUtils.encodeBase64(serializer.serialize(book)));

    // 3. json
    serializer = new TSerializer(new TJSONProtocol.Factory());
    bytes = serializer.serialize(book);
    serializationAndTest(parser, bytes);
  }

  @Test
  public void testDisableJavaScript()
  {
    final JavaScriptParseSpec parseSpec = new JavaScriptParseSpec(
        new TimestampSpec("timestamp", "auto", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(
                ImmutableList.of(
                    "dim1",
                    "dim2"
                )
            ),
            null,
            null
        ),
        "func",
        new JavaScriptConfig(false)
    );
    ThriftInputRowParser parser = new ThriftInputRowParser(
        parseSpec,
        "example/book.jar",
        "org.apache.druid.data.input.thrift.Book"
    );

    expectedException.expect(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage("JavaScript is disabled");

    //noinspection ResultOfMethodCallIgnored (this method call will trigger the expected exception)
    parser.parseBatch(ByteBuffer.allocate(1)).get(0);
  }

  private void serializationAndTest(ThriftInputRowParser parser, byte[] bytes)
  {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    InputRow row1 = parser.parseBatch(buffer).get(0);
    Assert.assertEquals("title", row1.getDimension("title").get(0));

    InputRow row2 = parser.parseBatch(new BytesWritable(bytes)).get(0);
    Assert.assertEquals("last", row2.getDimension("lastName").get(0));
  }
}
