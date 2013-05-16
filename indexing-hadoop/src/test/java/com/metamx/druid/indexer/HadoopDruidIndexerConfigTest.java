/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.indexer.partitions.PartitionsSpec;
import com.metamx.druid.indexer.updater.DbUpdaterJobSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;

import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testGranularitySpec() {
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
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-01-01/P1D")),
        granularitySpec.getIntervals()
    );

    Assert.assertEquals(
        "getGranularity",
        "HOUR",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testGranularitySpecLegacy() {
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
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-02-01/P1D")),
        granularitySpec.getIntervals()
    );

    Assert.assertEquals(
        "getGranularity",
        "DAY",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testGranularitySpecPostConstructorIntervals() {
    // Deprecated and replaced by granularitySpec, but still supported
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonMapper.readValue(
          "{"
          + "\"segmentGranularity\":\"day\""
          + "}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    cfg.setIntervals(Lists.newArrayList(new Interval("2012-03-01/P1D")));

    final UniformGranularitySpec granularitySpec = (UniformGranularitySpec) cfg.getGranularitySpec();

    Assert.assertEquals(
        "getIntervals",
        Lists.newArrayList(new Interval("2012-03-01/P1D")),
        granularitySpec.getIntervals()
    );

    Assert.assertEquals(
        "getGranularity",
        "DAY",
        granularitySpec.getGranularity().toString()
    );
  }

  @Test
  public void testInvalidGranularityCombination() {
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
    } catch(Exception e) {
      thrown = true;
    }

    Assert.assertTrue("Exception thrown", thrown);
  }

  @Test
  public void testPartitionsSpecAutoDimension() {
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
    } catch(Exception e) {
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
        "getPartitionDimension",
        partitionsSpec.getPartitionDimension(),
        null
    );
  }

  @Test
  public void testPartitionsSpecSpecificDimension() {
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
    } catch(Exception e) {
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

    Assert.assertEquals(
        "getPartitionDimension",
        partitionsSpec.getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testPartitionsSpecLegacy() {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{"
          + "\"targetPartitionSize\":100,"
          + "\"partitionDimension\":\"foo\""
          + "}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
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

    Assert.assertEquals(
        "getPartitionDimension",
        partitionsSpec.getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testPartitionsSpecMaxPartitionSize() {
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
    } catch(Exception e) {
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

    Assert.assertEquals(
        "getPartitionDimension",
        partitionsSpec.getPartitionDimension(),
        "foo"
    );
  }

  @Test
  public void testInvalidPartitionsCombination() {
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
    } catch(Exception e) {
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

    final DbUpdaterJobSpec spec = (DbUpdaterJobSpec) cfg.getUpdaterJobSpec();
    Assert.assertEquals("segments", spec.getSegmentTable());
    Assert.assertEquals("jdbc:mysql://localhost/druid", spec.getDatabaseConnectURI());
    Assert.assertEquals("rofl", spec.getDatabaseUser());
    Assert.assertEquals("p4ssw0rd", spec.getDatabasePassword());
    Assert.assertEquals(false, spec.useValidationQuery());
  }

  @Test
  public void testDefaultSettings() {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
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
  public void testNoCleanupOnFailure() {
    final HadoopDruidIndexerConfig cfg;

    try {
      cfg = jsonReadWriteRead(
          "{\"cleanupOnFailure\":false}",
          HadoopDruidIndexerConfig.class
      );
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }

    Assert.assertEquals(
        "cleanupOnFailure",
        cfg.isCleanupOnFailure(),
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
