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

package org.apache.druid.indexer.hbase.input;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation")
public class HBaseParseSpecTest
{
  private static final String HBASE_PARSE_SPEC_TEMPLATE = "{\n" +
      "          \"format\" : \"hbase\",\n" +
      "          \"timestampSpec\": {\n" +
      "            \"column\": \"time\",\n" +
      "            \"format\": \"iso\"\n" +
      "          },\n" +
      "          \"dimensionsSpec\" : {\n" +
      "            \"dimensions\" : [\n" +
      "              \"d1\",\n" +
      "              \"d2\"\n" +
      "            ]\n" +
      "          },\n" +
      "          \"hbaseRowSpec\": {\n" +
      "            \"rowKeySpec\": {\n" +
      "              \"format\": \"$$HBASE_ROW_SPEC_FORMAT$$\",\n" +
      "              \"delimiter\": \"|\",\n" +
      "              \"columns\": [\n" +
      "                {\n" +
      "                  \"type\": \"string\",\n" +
      "                  \"name\": \"salt\"\n" +
      "                },\n" +
      "                {\n" +
      "                  \"type\": \"string\",\n" +
      "                  \"name\": \"time\"\n" +
      "                }\n" +
      "              ]\n" +
      "            },\n" +
      "            \"columnSpec\": [\n" +
      "              {\n" +
      "                \"type\": \"string\",\n" +
      "                \"name\": \"A:q1\",\n" +
      "                \"mappingName\": \"d1\"\n" +
      "              },\n" +
      "              {\n" +
      "                \"type\": \"string\",\n" +
      "                \"name\": \"A:q2\",\n" +
      "                \"mappingName\": \"d2\"\n" +
      "              },\n" +
      "              {\n" +
      "                \"type\": \"string\",\n" +
      "                \"name\": \"A:q3\",\n" +
      "                \"mappingName\": \"m1\"\n" +
      "              }\n" +
      "            ]\n" +
      "          }\n" +
      "        }";

  private ObjectMapper mapper = new DefaultObjectMapper();
  private String specJson;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception
  {
  }

  @Before
  public void setUp() throws Exception
  {
    mapper.registerSubtypes(new NamedType(HBaseParseSpec.class, "hbase"));
    specJson = StringUtils.replace(HBASE_PARSE_SPEC_TEMPLATE, "$$HBASE_ROW_SPEC_FORMAT$$", "delimiter");
  }

  @After
  public void tearDown() throws Exception
  {
  }

  @Test
  public void testHBaseParseSpecConfig() throws IOException
  {
    ParseSpec parseSpec = mapper.convertValue(
        mapper.readValue(specJson, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        ParseSpec.class);

    Assert.assertThat(parseSpec, CoreMatchers.instanceOf(HBaseParseSpec.class));

    HBaseParseSpec hbaseParseSpec = (HBaseParseSpec) parseSpec;
    DimensionsSpec dimensionsSpec = hbaseParseSpec.getDimensionsSpec();
    TimestampSpec timestampSpec = hbaseParseSpec.getTimestampSpec();
    HBaseRowSpec hbaseRowSpec = hbaseParseSpec.getHbaseRowSpec();

    Assert.assertThat(dimensionsSpec.getDimensionNames(), CoreMatchers.hasItems("d1", "d2"));
    Assert.assertThat(timestampSpec.getTimestampColumn(), CoreMatchers.is("time"));

    HBaseRowKeySpec hbaseRowKeySpec = hbaseRowSpec.getRowKeySpec();
    Assert.assertThat(hbaseRowKeySpec, CoreMatchers.instanceOf(DelimiterHBaseRowKeySpec.class));
    Assert.assertThat(
        hbaseRowKeySpec.getRowkeySchemaList().stream().map(HBaseRowKeySchema::getName).collect(Collectors.toList()),
        CoreMatchers.hasItem("time"));

    Assert.assertThat(
        hbaseRowSpec.getColumnSchemaList().stream().map(HBaseColumnSchema::getName).collect(Collectors.toList()),
        CoreMatchers.hasItems("A:q1", "A:q2", "A:q3"));
    Assert.assertThat(
        hbaseRowSpec.getColumnSchemaList().stream().map(HBaseColumnSchema::getMappingName).collect(Collectors.toList()),
        CoreMatchers.hasItems("d1", "d2", "m1"));
  }

  @Test
  public void testFixedLenghHBaseRowKeySpecConfig() throws IOException
  {
    specJson = StringUtils.replace(HBASE_PARSE_SPEC_TEMPLATE, "$$HBASE_ROW_SPEC_FORMAT$$", "fixedLength");
    ParseSpec parseSpec = mapper.convertValue(
        mapper.readValue(specJson, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        ParseSpec.class);
    HBaseParseSpec hbaseParseSpec = (HBaseParseSpec) parseSpec;
    Assert.assertThat(hbaseParseSpec.getHbaseRowSpec().getRowKeySpec(),
        CoreMatchers.instanceOf(FixedLengthHBaseRowKeySpec.class));
  }
}
