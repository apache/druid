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

package org.apache.druid.indexer.hbase;

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexer.hbase.input.HBaseConnectionConfig;
import org.apache.druid.indexer.hbase.input.SnapshotScanInfo;
import org.apache.druid.indexer.hbase.input.TableScanInfo;
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
import java.util.Collections;

public class HBaseFirehoseConfigTest
{
  private static final String FIREHOSE_SPEC_JSON = "{\n" +
      "        \"type\": \"hbase\",\n" +
      "        \"connectionConfig\": {\n" +
      "          \"zookeeperQuorum\": \"zookeeper.quorum.com\",\n" +
      "          \"kerberosConfig\": {\n" +
      "            \"principal\": null,\n" +
      "            \"realm\": null,\n" +
      "            \"keytab\": null\n" +
      "          }\n" +
      "        },\n" +
      "        \"scanInfo\": {\n" +
      "          \"type\": \"$$SCAN_TYPE$$\",\n" +
      "          \"name\": \"wikipedia\",\n" +
      "          \"startKey\": null,\n" +
      "          \"endKey\": null\n" +
      "        },\n" +
      "        \"splitByRegion\": true,\n" +
      "        \"hbaseClientConfig\": {}\n" +
      "      }";

  ObjectMapper mapper = new DefaultObjectMapper();

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
    mapper.registerSubtypes(new NamedType(HBaseFirehoseFactory.class, "hbase"));
  }

  @After
  public void tearDown() throws Exception
  {
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testHBaseFirehoseSpecConfig() throws IOException
  {
    FirehoseFactory firehoseFactory = mapper.convertValue(
        mapper.readValue(FIREHOSE_SPEC_JSON, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        FirehoseFactory.class);

    Assert.assertThat(firehoseFactory, CoreMatchers.instanceOf(HBaseFirehoseFactory.class));

    HBaseFirehoseFactory hbaseFirehoseFactory = (HBaseFirehoseFactory) firehoseFactory;
    HBaseConnectionConfig connectionConfig = hbaseFirehoseFactory.getConnectionConfig();
    Assert.assertThat(connectionConfig, CoreMatchers.notNullValue());
    Assert.assertThat(connectionConfig.getZookeeperQuorum(), CoreMatchers.is("zookeeper.quorum.com"));

    Assert.assertThat(hbaseFirehoseFactory.getScanInfo(), CoreMatchers.notNullValue());
    Assert.assertThat(hbaseFirehoseFactory.isSplitByRegion(), CoreMatchers.is(true));
    Assert.assertThat(hbaseFirehoseFactory.getHbaseClientConfig(), CoreMatchers.is(Collections.EMPTY_MAP));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testHBaseTableFirehoseConfig() throws IOException
  {
    String specJson = StringUtils.replace(FIREHOSE_SPEC_JSON, "$$SCAN_TYPE$$", "table");
    FirehoseFactory firehoseFactory = mapper.convertValue(
        mapper.readValue(specJson, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        FirehoseFactory.class);

    HBaseFirehoseFactory hbaseFirehoseFactory = (HBaseFirehoseFactory) firehoseFactory;
    Assert.assertThat(hbaseFirehoseFactory.getScanInfo(), CoreMatchers.instanceOf(TableScanInfo.class));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testHBaseSnapshotFirehoseConfig() throws IOException
  {
    String specJson = StringUtils.replace(FIREHOSE_SPEC_JSON, "$$SCAN_TYPE$$", "snapshot");
    FirehoseFactory firehoseFactory = mapper.convertValue(
        mapper.readValue(specJson, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
        FirehoseFactory.class);

    HBaseFirehoseFactory hbaseFirehoseFactory = (HBaseFirehoseFactory) firehoseFactory;
    Assert.assertThat(hbaseFirehoseFactory.getScanInfo(), CoreMatchers.instanceOf(SnapshotScanInfo.class));
  }
}
