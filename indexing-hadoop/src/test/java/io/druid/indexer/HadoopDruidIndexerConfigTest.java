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
import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.Granularity;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.JSONDataSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class HadoopDruidIndexerConfigTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  public static <T> T jsonReadWriteRead(String s, Class<T> klass)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsBytes(jsonMapper.readValue(s, klass)), klass);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Test
  public void shouldMakeHDFSCompliantSegmentOutputPath()
  {
    HadoopIngestionSpec spec;

    try {
      spec = jsonReadWriteRead(
          "{"
          + "\"dataSource\": \"source\","
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-07-10/P1D\"]"
          + " },"
          + "\"segmentOutputPath\": \"hdfs://server:9100/tmp/druid/datatest\""
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        spec.withTuningConfig(
            spec.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        ),
        null
    );

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
    final HadoopIngestionSpec spec;

    try {
      spec = jsonReadWriteRead(
          "{"
          + "\"dataSource\": \"the:data:source\","
          + " \"granularitySpec\":{"
          + "   \"type\":\"uniform\","
          + "   \"gran\":\"hour\","
          + "   \"intervals\":[\"2012-07-10/P1D\"]"
          + " },"
          + "\"segmentOutputPath\": \"/tmp/dru:id/data:test\""
          + "}",
          HadoopIngestionSpec.class
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    HadoopDruidIndexerConfig cfg = new HadoopDruidIndexerConfig(
        spec.withTuningConfig(
            spec.getTuningConfig()
                  .withVersion(
                      "some:brand:new:version"
                  )
        ),
        null
    );

    Bucket bucket = new Bucket(4711, new DateTime(2012, 07, 10, 5, 30), 4712);
    Path path = cfg.makeSegmentOutputPath(new LocalFileSystem(), bucket);
    Assert.assertEquals(
        "/tmp/dru:id/data:test/the:data:source/2012-07-10T05:00:00.000Z_2012-07-10T06:00:00.000Z/some:brand:new:version/4712",
        path.toString()
    );

  }

  @Test
  public void testHashedBucketSelection() {
    List<HadoopyShardSpec> specs = Lists.newArrayList();
    final int partitionCount = 10;
    for (int i = 0; i < partitionCount; i++) {
      specs.add(new HadoopyShardSpec(new HashBasedNumberedShardSpec(i, partitionCount, new DefaultObjectMapper()), i));
    }
    HadoopIngestionSpec spec = new HadoopIngestionSpec(
        null, null, null,
        "foo",
        new TimestampSpec("timestamp", "auto"),
        new JSONDataSpec(ImmutableList.of("foo"), null),
        new UniformGranularitySpec(
            Granularity.HOUR,
            QueryGranularity.MINUTE,
            ImmutableList.of(new Interval("2010-01-01/P1D")),
            Granularity.HOUR
        ),
        null,
        null,
        null,
        null,
        null,
        false,
        true,
        ImmutableMap.of(new DateTime("2010-01-01T01:00:00"), specs),
        false,
        new DataRollupSpec(ImmutableList.<AggregatorFactory>of(), QueryGranularity.MINUTE),
        null,
        false,
        ImmutableMap.of("foo", "bar"),
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromSpec(spec);
    final List<String> dims = Arrays.asList("diM1", "dIM2");
    final ImmutableMap<String, Object> values = ImmutableMap.<String, Object>of(
        "Dim1",
        "1",
        "DiM2",
        "2",
        "dim1",
        "3",
        "dim2",
        "4"
    );
    final long timestamp = new DateTime("2010-01-01T01:00:01").getMillis();
    final Bucket expectedBucket = config.getBucket(new MapBasedInputRow(timestamp, dims, values)).get();
    final long nextBucketTimestamp = QueryGranularity.MINUTE.next(QueryGranularity.MINUTE.truncate(timestamp));
    // check that all rows having same set of dims and truncated timestamp hash to same bucket
    for (int i = 0; timestamp + i < nextBucketTimestamp; i++) {
      Assert.assertEquals(
          expectedBucket.partitionNum,
          config.getBucket(new MapBasedInputRow(timestamp + i, dims, values)).get().partitionNum
      );
    }

  }
}
