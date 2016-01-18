/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class HadoopIngestionSpecTest
{
  private static final ObjectMapper jsonMapper;
  static {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, jsonMapper));
  }

  @Test
  public void testGranularitySpec()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{\n"
          + "    \"dataSchema\": {\n"
          + "     \"dataSource\": \"foo\",\n"
          + "     \"metricsSpec\": [],\n"
          + "        \"granularitySpec\": {\n"
          + "                \"type\": \"uniform\",\n"
          + "                \"segmentGranularity\": \"hour\",\n"
          + "                \"intervals\": [\"2012-01-01/P1D\"]\n"
          + "        }\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) schema.getDataSchema().getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-01-01/P1D")),
        granularitySpec.getIntervals().get()
    );

    Assert.assertEquals(
        "getSegmentGranularity",
        "HOUR",
        granularitySpec.getSegmentGranularity().toString()
    );
  }

  @Test
  public void testPartitionsSpecAutoHashed()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{\n"
          + "    \"tuningConfig\": {\n"
          + "        \"type\": \"hadoop\",\n"
          + "        \"partitionsSpec\": {\n"
          + "            \"targetPartitionSize\": 100\n"
          + "        }\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = schema.getTuningConfig().getPartitionsSpec();

    Assert.assertEquals(
        "isDeterminingPartitions",
        partitionsSpec.isDeterminingPartitions(),
        true
    );

    Assert.assertEquals(
        "getTargetPartitionSize",
        partitionsSpec.getTargetPartitionSize(),
        100
    );

    Assert.assertTrue(
        "partitionSpec",
        partitionsSpec instanceof HashedPartitionsSpec
    );
  }

  @Test
  public void testPartitionsSpecMaxPartitionSize()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(

          "{\n"
          + "    \"tuningConfig\": {\n"
          + "        \"type\": \"hadoop\",\n"
          + "        \"partitionsSpec\": {\n"
          + "            \"type\": \"dimension\",\n"
          + "            \"targetPartitionSize\": 100,\n"
          + "            \"maxPartitionSize\" : 200,\n"
          + "            \"partitionDimension\" : \"foo\"\n"
          + "        }\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = schema.getTuningConfig().getPartitionsSpec();

    Assert.assertEquals(
        "isDeterminingPartitions",
        partitionsSpec.isDeterminingPartitions(),
        true
    );

    Assert.assertEquals(
        "getTargetPartitionSize",
        partitionsSpec.getTargetPartitionSize(),
        100
    );

    Assert.assertEquals(
        "getMaxPartitionSize",
        partitionsSpec.getMaxPartitionSize(),
        200
    );

    Assert.assertTrue("partitionsSpec" , partitionsSpec instanceof SingleDimensionPartitionsSpec);
    Assert.assertEquals(
        "getPartitionDimension",
        ((SingleDimensionPartitionsSpec)partitionsSpec).getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testDbUpdaterJobSpec() throws Exception
  {
    final HadoopIngestionSpec schema;

    schema = jsonReadWriteRead(
        "{\n"
        + "    \"ioConfig\": {\n"
        + "        \"type\": \"hadoop\",\n"
        + "        \"metadataUpdateSpec\": {\n"
        + "            \"type\": \"db\",\n"
        + "            \"connectURI\": \"jdbc:mysql://localhost/druid\",\n"
        + "            \"user\": \"rofl\",\n"
        + "            \"password\": \"p4ssw0rd\",\n"
        + "            \"segmentTable\": \"segments\"\n"
        + "        }\n"
        + "    }\n"
        + "}",
        HadoopIngestionSpec.class
    );

    final MetadataStorageUpdaterJobSpec spec = schema.getIOConfig().getMetadataUpdateSpec();
    final MetadataStorageConnectorConfig connectorConfig = spec.get();

    Assert.assertEquals("segments", spec.getSegmentTable());
    Assert.assertEquals("jdbc:mysql://localhost/druid", connectorConfig.getConnectURI());
    Assert.assertEquals("rofl", connectorConfig.getUser());
    Assert.assertEquals("p4ssw0rd", connectorConfig.getPassword());
  }

  @Test
  public void testDefaultSettings()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(
        "cleanupOnFailure",
        schema.getTuningConfig().isCleanupOnFailure(),
        true
    );

    Assert.assertEquals(
        "overwriteFiles",
        schema.getTuningConfig().isOverwriteFiles(),
        false
    );

    Assert.assertEquals(
        "isDeterminingPartitions",
        schema.getTuningConfig().getPartitionsSpec().isDeterminingPartitions(),
        false
    );

    Assert.assertFalse(Strings.isNullOrEmpty(schema.getUniqueId()));
  }

  @Test
  public void testUniqueId() throws Exception
  {
    final HadoopIngestionSpec schema = jsonReadWriteRead(
        "{\"uniqueId\" : \"test_unique_id\"}",
        HadoopIngestionSpec.class
    );

    Assert.assertEquals("test_unique_id", schema.getUniqueId());

    //test uniqueId assigned is really unique
    final String id1 = jsonReadWriteRead(
        "{}",
        HadoopIngestionSpec.class
    ).getUniqueId();

    final String id2 = jsonReadWriteRead(
        "{}",
        HadoopIngestionSpec.class
    ).getUniqueId();

    Assert.assertNotEquals(id1, id2);
  }

  @Test
  public void testNoCleanupOnFailure()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{\n"
          + "    \"tuningConfig\" : {\n"
          + "        \"type\" : \"hadoop\", \n"
          + "        \"cleanupOnFailure\" : \"false\"\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(
        "cleanupOnFailure",
        schema.getTuningConfig().isCleanupOnFailure(),
        false
    );
  }

  private <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

}
