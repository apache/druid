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

package org.apache.druid.indexer.hbase2;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.indexer.hbase2.input.HBaseInputRowParser;
import org.apache.druid.indexer.hbase2.input.HBaseParseSpec;
import org.apache.druid.indexer.hbase2.path.HBasePathSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.segment.transform.TransformingInputRowParser;
import org.apache.druid.server.security.AuthorizerMapper;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

public class HadoopHBaseIndexTaskConfigTest
{
  private static final String INDEX_HADOOP_HBASE_JSON = "{\n" + 
      "  \"type\": \"index_hadoop_hbase2\",\n" + 
      "  \"spec\" : {\n" + 
      "    \"dataSchema\" : {\n" + 
      "      \"dataSource\" : \"wikipedia-hadoop-hbase\",\n" + 
      "      \"parser\" : {\n" + 
      "        \"type\" : \"hbase2\",\n" + 
      "        \"parseSpec\" : {\n" + 
      "          \"format\" : \"hbase2\",\n" + 
      "          \"dimensionsSpec\" : {\n" + 
      "            \"dimensions\" : [\n" + 
      "              \"dim1\"\n" + 
      "            ]\n" + 
      "          },\n" + 
      "          \"timestampSpec\" : {\n" + 
      "            \"format\" : \"auto\",\n" + 
      "            \"column\" : \"time\"\n" + 
      "          },\n" + 
      "          \"hbaseRowSpec\": {\n" + 
      "            \"rowKeySpec\": {\n" + 
      "              \"format\": \"delimiter\",\n" + 
      "              \"delimiter\": \"|\",\n" + 
      "              \"columns\": [\n" + 
      "                {\n" + 
      "                  \"type\": \"string\",\n" + 
      "                  \"name\": \"row1\"\n" + 
      "                }\n" + 
      "              ]\n" + 
      "            },\n" + 
      "            \"columnSpec\": [\n" + 
      "              {\n" + 
      "                \"type\": \"string\",\n" + 
      "                \"name\": \"A:q1\",\n" + 
      "                \"mappingName\": \"q1\"\n" + 
      "              }\n" + 
      "            ]\n" + 
      "          }\n" + 
      "        }\n" + 
      "      }\n" + 
      "    },\n" +
      "    \"ioConfig\": {\n" +
      "      \"type\": \"hadoop\",\n" +
      "      \"inputSpec\": {\n" +
      "        \"type\": \"hbase2\"\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" + 
      "}";

  private static final String INPUT_PATH_JSON = "{\n" +
      "        \"type\" : \"hbase2\",\n" +
      "        \"connectionConfig\": {\n" +
      "          \"zookeeperQuorum\": \"zookeeper.quorum.com\",\n" +
      "          \"kerberosConfig\": {\n" +
      "            \"principal\": null,\n" +
      "            \"realm\": null,\n" +
      "            \"keytab\": null\n" +
      "          }\n" +
      "        },\n" +
      "        \"scanInfo\": {\n" +
      "          \"type\": \"table\",\n" +
      "          \"name\": \"default:wikipedia\",\n" +
      "          \"startKey\": null,\n" +
      "          \"endKey\": null\n" +
      "        },\n" +
      "        \"hbaseClientConfig\": {\n" +
      "        }\n" +
      "}";

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
  {
    InjectableValues injectableValues = new InjectableValues.Std()
        .addValue(ObjectMapper.class, MAPPER)
        .addValue(AuthorizerMapper.class, null)
        .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider());
    MAPPER.setInjectableValues(injectableValues);

    MAPPER.registerSubtypes(new NamedType(HadoopHBaseIndexTask.class, "index_hadoop_hbase2"),
        new NamedType(HBasePathSpec.class, "hbase2"),
        new NamedType(HBaseInputRowParser.class, "hbase2"),
        new NamedType(HBaseParseSpec.class, "hbase2"));
  }

  @Test
  public void testHadoopHBaseIndexTaskConfig() throws Exception
  {
    HadoopHBaseIndexTask hadoopHBaseIndexTask = MAPPER.convertValue(
        MAPPER.readValue(INDEX_HADOOP_HBASE_JSON, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        HadoopHBaseIndexTask.class);
    Assert.assertThat(hadoopHBaseIndexTask, CoreMatchers.instanceOf(HadoopHBaseIndexTask.class));

    // The inputRowParser is probably a TransformingInputRowParser wrapping an HBaseInputRowParser.
    InputRowParser inputRowParser = hadoopHBaseIndexTask.getSpec().getDataSchema().getParser();
    Field parserField = TransformingInputRowParser.class.getDeclaredField("parser");
    parserField.setAccessible(true);
    Object obj = parserField.get(inputRowParser);
    Assert.assertThat(obj, CoreMatchers.instanceOf(HBaseInputRowParser.class));

    Assert.assertThat(inputRowParser.getParseSpec(), CoreMatchers.instanceOf(HBaseParseSpec.class));
  }

  @Test
  public void testInputSpecConfig() throws IOException
  {
    HBasePathSpec hbasePathSpec = MAPPER.convertValue(
        MAPPER.readValue(INPUT_PATH_JSON, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        HBasePathSpec.class);

    Assert.assertThat(hbasePathSpec, CoreMatchers.instanceOf(HBasePathSpec.class));
    Assert.assertThat("zookeeper.quorum.com", CoreMatchers.is(hbasePathSpec.getConnectionConfig().getZookeeperQuorum()));
    Assert.assertThat(hbasePathSpec.getScanInfo(), CoreMatchers.notNullValue());
    Assert.assertThat(hbasePathSpec.getHbaseClientConfig(), CoreMatchers.notNullValue());
  }
}
