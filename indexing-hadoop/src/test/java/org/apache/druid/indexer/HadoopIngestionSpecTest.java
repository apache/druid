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

package org.apache.druid.indexer;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexer.updater.MetadataStorageUpdaterJobSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class HadoopIngestionSpecTest
{
  private static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, JSON_MAPPER));
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
      throw new RuntimeException(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) schema.getDataSchema().getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Collections.singletonList(Intervals.of("2012-01-01/P1D")),
        granularitySpec.inputIntervals()
    );

    Assert.assertEquals(
        "getSegmentGranularity",
        Granularities.HOUR,
        granularitySpec.getSegmentGranularity()
    );
  }

  @Test
  public void testPeriodSegmentGranularitySpec()
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
          + "                \"segmentGranularity\": {\"type\": \"period\", \"period\":\"PT1H\", \"timeZone\":\"America/Los_Angeles\"},\n"
          + "                \"intervals\": [\"2012-01-01/P1D\"]\n"
          + "        }\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) schema.getDataSchema().getGranularitySpec();

    Assert.assertEquals(
        "getSegmentGranularity",
        new PeriodGranularity(new Period("PT1H"), null, DateTimes.inferTzFromString("America/Los_Angeles")),
        granularitySpec.getSegmentGranularity()
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
      throw new RuntimeException(e);
    }

    final PartitionsSpec partitionsSpec = schema.getTuningConfig().getPartitionsSpec();

    Assert.assertTrue("isDeterminingPartitions", partitionsSpec.needsDeterminePartitions(true));

    Assert.assertEquals(
        "getTargetPartitionSize",
        100,
        partitionsSpec.getMaxRowsPerSegment().intValue()
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
          + "            \"maxPartitionSize\" : null,\n"
          + "            \"partitionDimension\" : \"foo\"\n"
          + "        }\n"
          + "    }\n"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    PartitionsSpec partitionsSpec = schema.getTuningConfig().getPartitionsSpec();
    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof SingleDimensionPartitionsSpec);

    SingleDimensionPartitionsSpec singleDimensionPartitionsSpec = (SingleDimensionPartitionsSpec) partitionsSpec;

    Assert.assertTrue("isDeterminingPartitions", singleDimensionPartitionsSpec.needsDeterminePartitions(true));

    Assert.assertEquals(
        "getTargetPartitionSize",
        100,
        singleDimensionPartitionsSpec.getTargetRowsPerSegment().intValue()
    );

    Assert.assertEquals(
        "getMaxPartitionSize",
        150,
        singleDimensionPartitionsSpec.getMaxRowsPerSegment().intValue()
    );

    Assert.assertEquals(
        "getPartitionDimension",
        "foo",
        singleDimensionPartitionsSpec.getPartitionDimension()
    );
  }

  @Test
  public void testDbUpdaterJobSpec()
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
      throw new RuntimeException(e);
    }

    Assert.assertEquals(
        "cleanupOnFailure",
        true,
        schema.getTuningConfig().isCleanupOnFailure()
    );

    Assert.assertFalse("overwriteFiles", schema.getTuningConfig().isOverwriteFiles());

    Assert.assertTrue(
        "isDeterminingPartitions",
        schema.getTuningConfig().getPartitionsSpec().needsDeterminePartitions(true)
    );

    Assert.assertFalse(Strings.isNullOrEmpty(schema.getUniqueId()));
  }

  @Test
  public void testUniqueId()
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
      throw new RuntimeException(e);
    }

    Assert.assertFalse("cleanupOnFailure", schema.getTuningConfig().isCleanupOnFailure());
  }

  private static <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsBytes(JSON_MAPPER.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
