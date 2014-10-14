/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.db.DbConnectorConfig;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.partitions.RandomPartitionsSpec;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class HadoopIngestionSpecTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testGranularitySpec()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-01-01/P1D\"]"
          + " }"
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
  public void testGranularitySpecLegacy()
  {
    // Deprecated and replaced by granularitySpec, but still supported
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
          + "\"segmentGranularity\":\"day\","
          + "\"intervals\":[\"2012-02-01/P1D\"]"
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
        Lists.newArrayList(new Interval("2012-02-01/P1D")),
        granularitySpec.getIntervals().get()
    );

    Assert.assertEquals(
        "getSegmentGranularity",
        "DAY",
        granularitySpec.getSegmentGranularity().toString()
    );
  }

  @Test
  public void testInvalidGranularityCombination()
  {
    boolean thrown = false;
    try {
      final HadoopIngestionSpec schema = jsonReadWriteRead(
          "{"
          + "\"segmentGranularity\":\"day\","
          + "\"intervals\":[\"2012-02-01/P1D\"],"
          + "\"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-01-01/P1D\"]"
          + " }"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testPartitionsSpecAutoDimension()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100"
          + " }"
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
        partitionsSpec instanceof SingleDimensionPartitionsSpec
    );
  }

  @Test
  public void testPartitionsSpecSpecificDimensionLegacy()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100,"
          + "   \"partitionDimension\":\"foo\""
          + " }"
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
        150
    );

    Assert.assertTrue("partitionsSpec" , partitionsSpec instanceof SingleDimensionPartitionsSpec);
    Assert.assertEquals(
        "getPartitionDimension",
        ((SingleDimensionPartitionsSpec)partitionsSpec).getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testPartitionsSpecLegacy()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
          + "\"targetPartitionSize\":100,"
          + "\"partitionDimension\":\"foo\""
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
        150
    );

    Assert.assertTrue("partitionsSpec" , partitionsSpec instanceof SingleDimensionPartitionsSpec);
    Assert.assertEquals(
        "getPartitionDimension",
        ((SingleDimensionPartitionsSpec)partitionsSpec).getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testPartitionsSpecMaxPartitionSize()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100,"
          + "   \"maxPartitionSize\":200,"
          + "   \"partitionDimension\":\"foo\""
          + " }"
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
  public void testInvalidPartitionsCombination()
  {
    boolean thrown = false;
    try {
      final HadoopIngestionSpec schema = jsonReadWriteRead(
          "{"
          + "\"targetPartitionSize\":100,"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100"
          + " }"
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testDbUpdaterJobSpec() throws Exception
  {
    final HadoopIngestionSpec schema;

    schema = jsonReadWriteRead(
        "{"
        + "\"updaterJobSpec\":{\n"
        + "    \"type\" : \"db\",\n"
        + "    \"connectURI\" : \"jdbc:mysql://localhost/druid\",\n"
        + "    \"user\" : \"rofl\",\n"
        + "    \"password\" : \"p4ssw0rd\",\n"
        + "    \"segmentTable\" : \"segments\"\n"
        + "  }"
        + "}",
        HadoopIngestionSpec.class
    );

    final DbUpdaterJobSpec spec = schema.getIOConfig().getMetadataUpdateSpec();
    final DbConnectorConfig connectorConfig = spec.get();

    Assert.assertEquals("segments", spec.getSegmentTable());
    Assert.assertEquals("jdbc:mysql://localhost/druid", connectorConfig.getConnectURI());
    Assert.assertEquals("rofl", connectorConfig.getUser());
    Assert.assertEquals("p4ssw0rd", connectorConfig.getPassword());
    Assert.assertEquals(false, connectorConfig.isUseValidationQuery());
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
  }

  @Test
  public void testNoCleanupOnFailure()
  {
    final HadoopIngestionSpec schema;

    try {
      schema = jsonReadWriteRead(
          "{\"cleanupOnFailure\":false}",
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

  public void testRandomPartitionsSpec() throws Exception{
    {
      final HadoopIngestionSpec schema;

      try {
        schema = jsonReadWriteRead(
            "{"
            + "\"partitionsSpec\":{"
            + "   \"targetPartitionSize\":100,"
            + "   \"type\":\"random\""
            + " }"
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
          150
      );

      Assert.assertTrue("partitionsSpec" , partitionsSpec instanceof RandomPartitionsSpec);
    }
  }
}
