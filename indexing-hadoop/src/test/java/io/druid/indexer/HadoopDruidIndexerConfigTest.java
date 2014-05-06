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
import io.druid.indexer.granularity.UniformGranularitySpec;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.partitions.RandomPartitionsSpec;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testGranularitySpec()
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-01-01/P1D\"]"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-01-01/P1D")),
        granularitySpec.getIntervals().get()
    );

    Assert.assertEquals(
        "getGranularity",
        "HOUR",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testGranularitySpecLegacy()
  {
    // Deprecated and replaced by granularitySpec, but still supported
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"segmentGranularity\":\"day\","
          + "\"intervals\":[\"2012-02-01/P1D\"]"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-02-01/P1D")),
        granularitySpec.getIntervals().get()
    );

    Assert.assertEquals(
        "getGranularity",
        "DAY",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testInvalidGranularityCombination()
  {
    boolean thrown = false;
    try {
      final HadoopDruidIndexerConfig cfg = jsonReadWriteRead(
          "{"
          + "\"segmentGranularity\":\"day\","
          + "\"intervals\":[\"2012-02-01/P1D\"],"
          + "\"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-01-01/P1D\"]"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
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
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

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
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100,"
          + "   \"partitionDimension\":\"foo\""
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

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

    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof SingleDimensionPartitionsSpec);
    Assert.assertEquals(
        "getPartitionDimension",
        ((SingleDimensionPartitionsSpec) partitionsSpec).getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testPartitionsSpecLegacy()
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"targetPartitionSize\":100,"
          + "\"partitionDimension\":\"foo\""
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

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

    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof SingleDimensionPartitionsSpec);
    Assert.assertEquals(
        "getPartitionDimension",
        ((SingleDimensionPartitionsSpec) partitionsSpec).getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testPartitionsSpecMaxPartitionSize()
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100,"
          + "   \"maxPartitionSize\":200,"
          + "   \"partitionDimension\":\"foo\""
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

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

    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof SingleDimensionPartitionsSpec);
    Assert.assertEquals(
        "getPartitionDimension",
        ((SingleDimensionPartitionsSpec) partitionsSpec).getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testInvalidPartitionsCombination()
  {
    boolean thrown = false;
    try {
      final HadoopDruidIndexerConfig cfg = jsonReadWriteRead(
          "{"
          + "\"targetPartitionSize\":100,"
          + "\"partitionsSpec\":{"
          + "   \"targetPartitionSize\":100"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
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
    final HadoopDruidIndexerConfig cfg;

    cfg = jsonReadWriteRead(
        "{"
        + "\"updaterJobSpec\":{\n"
        + "    \"type\" : \"db\",\n"
        + "    \"connectURI\" : \"jdbc:mysql://localhost/druid\",\n"
        + "    \"user\" : \"rofl\",\n"
        + "    \"password\" : \"p4ssw0rd\",\n"
        + "    \"segmentTable\" : \"segments\"\n"
        + "  }"
        + "}",
        HadoopDruidIndexerConfig.class
    );

    final DbUpdaterJobSpec spec = cfg.getUpdaterJobSpec();
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
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(
        "cleanupOnFailure",
        cfg.isCleanupOnFailure(),
        true
    );

    Assert.assertEquals(
        "overwriteFiles",
        cfg.isOverwriteFiles(),
        false
    );

    Assert.assertEquals(
        "isDeterminingPartitions",
        cfg.getPartitionsSpec().isDeterminingPartitions(),
        false
    );
  }

  @Test
  public void testNoCleanupOnFailure()
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{\"cleanupOnFailure\":false}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(
        "cleanupOnFailure",
        cfg.isCleanupOnFailure(),
        false
    );
  }

  @Test
  public void shouldMakeHDFSCompliantSegmentOutputPath()
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"dataSource\": \"source\","
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-07-10/P1D\"]"
          + " },"
          + "\"segmentOutputPath\": \"hdfs://server:9100/tmp/druid/datatest\""
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    cfg.setVersion("some:brand:new:version");

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new DistributedFileSystem(), bucket);
    Assert.assertEquals(
        "hdfs://server:9100/tmp/druid/datatest/source/20120710T050000.000Z_20120710T060000.000Z/some_brand_new_version/4712",
        path.toString()
    );
  }

  @Test
  public void shouldMakeDefaultSegmentOutputPathIfNotHDFS()
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"dataSource\": \"the:data:source\","
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-07-10/P1D\"]"
          + " },"
          + "\"segmentOutputPath\": \"/tmp/dru:id/data:test\""
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    cfg.setVersion("some:brand:new:version");

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new LocalFileSystem(), bucket);
    Assert.assertEquals(
        "/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:version/4712",
        path.toString()
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

  @Test
  public void testRandomPartitionsSpec() throws Exception
  {
    {
      final HadoopDruidIndexerConfig cfg;

      try {
        cfg = jsonReadWriteRead(
            "{"
            + "\"partitionsSpec\":{"
            + "   \"targetPartitionSize\":100,"
            + "   \"type\":\"random\""
            + " }"
            + "}",
            HadoopDruidIndexerConfig.class
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

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

      Assert.assertTrue("partitionsSpec", partitionsSpec instanceof RandomPartitionsSpec);
    }
  }

  @Test
  public void testHashedPartitionsSpec() throws Exception
  {
    {
      final HadoopDruidIndexerConfig cfg;

      try {
        cfg = jsonReadWriteRead(
            "{"
            + "\"partitionsSpec\":{"
            + "   \"targetPartitionSize\":100,"
            + "   \"type\":\"hashed\""
            + " }"
            + "}",
            HadoopDruidIndexerConfig.class
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

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

      Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);
    }
  }

  @Test
  public void testHashedPartitionsSpecShardCount() throws Exception
  {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"partitionsSpec\":{"
          + "   \"type\":\"hashed\","
          + "   \"numShards\":2"
          + " }"
          + "}",
          HadoopDruidIndexerConfig.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    final PartitionsSpec partitionsSpec = cfg.getPartitionsSpec();

    Assert.assertEquals(
        "isDeterminingPartitions",
        partitionsSpec.isDeterminingPartitions(),
        false
    );

    Assert.assertEquals(
        "getTargetPartitionSize",
        partitionsSpec.getTargetPartitionSize(),
        -1
    );

    Assert.assertEquals(
        "getMaxPartitionSize",
        partitionsSpec.getMaxPartitionSize(),
        -1
    );

    Assert.assertEquals(
        "shardCount",
        partitionsSpec.getNumShards(),
        2
    );

    Assert.assertTrue("partitionsSpec", partitionsSpec instanceof HashedPartitionsSpec);

  }
}
