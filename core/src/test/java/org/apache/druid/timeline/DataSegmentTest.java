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

package org.apache.druid.timeline;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import org.apache.druid.TestObjectMapper;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class DataSegmentTest
{
  private static final ObjectMapper MAPPER = new TestObjectMapper();
  private static final int TEST_VERSION = 0x9;

  private static ShardSpec getShardSpec(final int partitionNum)
  {
    return new ShardSpec()
    {
      @Override
      public <T> PartitionChunk<T> createChunk(T obj)
      {
        return null;
      }

      @Override
      public boolean isInChunk(long timestamp, InputRow inputRow)
      {
        return false;
      }

      @Override
      public int getPartitionNum()
      {
        return partitionNum;
      }

      @Override
      public int getNumCorePartitions()
      {
        return 0;
      }

      @Override
      public ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
      {
        return null;
      }

      @Override
      public List<String> getDomainDimensions()
      {
        return ImmutableList.of();
      }

      @Override
      public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
      {
        return true;
      }
    };
  }

  @Before
  public void setUp()
  {
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    MAPPER.setInjectableValues(injectableValues);
  }

  @Test
  public void testV1Serialization() throws Exception
  {

    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        new NumberedShardSpec(3, 0),
        new CompactionState(
            new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
            ImmutableMap.of()
        ),
        TEST_VERSION,
        1
    );

    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segment),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(11, objectMap.size());
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0), objectMap.get("shardSpec"));
    Assert.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));

    DataSegment deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);

    Assert.assertEquals(segment.getDataSource(), deserializedSegment.getDataSource());
    Assert.assertEquals(segment.getInterval(), deserializedSegment.getInterval());
    Assert.assertEquals(segment.getVersion(), deserializedSegment.getVersion());
    Assert.assertEquals(segment.getLoadSpec(), deserializedSegment.getLoadSpec());
    Assert.assertEquals(segment.getDimensions(), deserializedSegment.getDimensions());
    Assert.assertEquals(segment.getMetrics(), deserializedSegment.getMetrics());
    Assert.assertEquals(segment.getShardSpec(), deserializedSegment.getShardSpec());
    Assert.assertEquals(segment.getSize(), deserializedSegment.getSize());
    Assert.assertEquals(segment.getId(), deserializedSegment.getId());

    deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(0, segment.compareTo(deserializedSegment));

    deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(0, deserializedSegment.compareTo(segment));

    deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testIdentifier()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(NoneShardSpec.instance())
                                           .size(0)
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z",
        segment.getId().toString()
    );
  }

  @Test
  public void testIdentifierWithZeroPartition()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(getShardSpec(0))
                                           .size(0)
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z",
        segment.getId().toString()
    );
  }

  @Test
  public void testIdentifierWithNonzeroPartition()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(getShardSpec(7))
                                           .size(0)
                                           .build();

    Assert.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z_7",
        segment.getId().toString()
    );
  }

  @Test
  public void testV1SerializationNullMetrics() throws Exception
  {
    final DataSegment segment =
        makeDataSegment("foo", "2012-01-01/2012-01-02", DateTimes.of("2012-01-01T11:22:33.444Z").toString());

    final DataSegment segment2 = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals("empty dimensions", ImmutableList.of(), segment2.getDimensions());
    Assert.assertEquals("empty metrics", ImmutableList.of(), segment2.getMetrics());
  }

  private DataSegment makeDataSegment(String dataSource, String interval, String version)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(Intervals.of(interval))
                      .version(version)
                      .size(1)
                      .build();
  }
}
