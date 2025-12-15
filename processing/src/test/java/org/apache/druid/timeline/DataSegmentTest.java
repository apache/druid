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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Unit tests for {@link DataSegment}, covering construction, serialization, and utility methods.
 */
public class DataSegmentTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
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
  public void testSerializationWithProjections() throws Exception
  {
    // arrange
    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ShardSpec shardSpec = new NumberedShardSpec(3, 0);
    final SegmentId segmentId = SegmentId.of("something", interval, "1", shardSpec);

    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");
    final CompactionState compactionState = new CompactionState(
        new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))),
        ImmutableList.of(new CountAggregatorFactory("count")),
        new CompactionTransformSpec(new SelectorDimFilter("dim1", "foo", null)),
        MAPPER.convertValue(ImmutableMap.of(), IndexSpec.class),
        MAPPER.convertValue(ImmutableMap.of(), GranularitySpec.class),
        null
    );
    final DataSegment segment = DataSegment.builder(segmentId)
                                           .loadSpec(loadSpec)
                                           .dimensions(Arrays.asList("dim1", "dim2"))
                                           .metrics(Arrays.asList("met1", "met2"))
                                           .projections(Arrays.asList("proj1", "proj2"))
                                           .shardSpec(shardSpec)
                                           .lastCompactionState(compactionState)
                                           .binaryVersion(TEST_VERSION)
                                           .size(1)
                                           .build();
    // act & assert
    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segment),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertEquals(12, objectMap.size());
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals("proj1,proj2", objectMap.get("projections"));
    Assert.assertEquals(
        ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0),
        objectMap.get("shardSpec")
    );
    Assert.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
    Assert.assertEquals(6, ((Map) objectMap.get("lastCompactionState")).size());
    // another act & assert
    DataSegment deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    assertAllFieldsEquals(segment, deserializedSegment);
    Assert.assertEquals(0, segment.compareTo(deserializedSegment));
    Assert.assertEquals(0, deserializedSegment.compareTo(segment));
    Assert.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
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
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))
            ),
            ImmutableList.of(new CountAggregatorFactory("count")),
            new CompactionTransformSpec(new SelectorDimFilter("dim1", "foo", null)),
            MAPPER.convertValue(ImmutableMap.of(), IndexSpec.class),
            MAPPER.convertValue(ImmutableMap.of(), GranularitySpec.class),
            null
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
    Assert.assertEquals(
        ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0),
        objectMap.get("shardSpec")
    );
    Assert.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
    Assert.assertEquals(6, ((Map) objectMap.get("lastCompactionState")).size());

    DataSegment deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    assertAllFieldsEquals(segment, deserializedSegment);
    Assert.assertEquals(0, segment.compareTo(deserializedSegment));
    Assert.assertEquals(0, deserializedSegment.compareTo(segment));
    Assert.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testDeserializationDataSegmentLastCompactionStateWithNullSpecs() throws Exception
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
            null,
            null,
            null,
            MAPPER.convertValue(ImmutableMap.of(), IndexSpec.class),
            MAPPER.convertValue(ImmutableMap.of(), GranularitySpec.class),
            null
        ),
        TEST_VERSION,
        1
    );
    // lastCompactionState has null specs for dimensionSpec and transformSpec
    String lastCompactionStateWithNullSpecs = "{"
                                              + "\"dataSource\": \"something\","
                                              + "\"interval\": \"2011-10-01T00:00:00.000Z/2011-10-02T00:00:00.000Z\","
                                              + "\"version\": \"1\","
                                              + "\"loadSpec\": {"
                                              + " \"something\": \"or_other\""
                                              + "},"
                                              + "\"dimensions\": \"dim1,dim2\","
                                              + "\"metrics\": \"met1,met2\","
                                              + "\"shardSpec\": {"
                                              + " \"type\": \"numbered\","
                                              + " \"partitionNum\": 3,"
                                              + " \"partitions\": 0"
                                              + "},"
                                              + "\"lastCompactionState\": {"
                                              + " \"partitionsSpec\": {"
                                              + "  \"type\": \"hashed\","
                                              + "  \"numShards\": null,"
                                              + "  \"partitionDimensions\": [\"dim1\"],"
                                              + "  \"partitionFunction\": \"murmur3_32_abs\","
                                              + "  \"maxRowsPerSegment\": 100000"
                                              + " },"
                                              + " \"indexSpec\": {},"
                                              + " \"granularitySpec\": {}"
                                              + "},"
                                              + "\"binaryVersion\": 9,"
                                              + "\"size\": 1,"
                                              + "\"identifier\": \"something_2011-10-01T00:00:00.000Z_2011-10-02T00:00:00.000Z_1_3\""
                                              + "}";

    final Map<String, Object> objectMap = MAPPER.readValue(
        lastCompactionStateWithNullSpecs,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertEquals(11, objectMap.size());
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(
        ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0),
        objectMap.get("shardSpec")
    );
    Assert.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
    Assert.assertEquals(3, ((Map) objectMap.get("lastCompactionState")).size());

    DataSegment deserializedSegment = MAPPER.readValue(lastCompactionStateWithNullSpecs, DataSegment.class);
    assertAllFieldsEquals(segment, deserializedSegment);
    Assert.assertNotNull(segment.getLastCompactionState());
    Assert.assertNull(segment.getLastCompactionState().getDimensionsSpec());
    Assert.assertNull(segment.getLastCompactionState().getTransformSpec());
    Assert.assertNull(segment.getLastCompactionState().getMetricsSpec());
    Assert.assertNotNull(deserializedSegment.getLastCompactionState());
    Assert.assertNull(deserializedSegment.getLastCompactionState().getDimensionsSpec());

    Assert.assertEquals(0, segment.compareTo(deserializedSegment));
    Assert.assertEquals(0, deserializedSegment.compareTo(segment));
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
    final DataSegment segment = DataSegment.builder(SegmentId.of(
        "foo",
        Intervals.of("2012-01-01/2012-01-02"),
        DateTimes.of("2012-01-01T11:22:33.444Z").toString(),
        null
    )).size(1).build();

    final DataSegment segment2 = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals("empty dimensions", ImmutableList.of(), segment2.getDimensions());
    Assert.assertEquals("empty metrics", ImmutableList.of(), segment2.getMetrics());
  }

  @Test
  public void testWithLastCompactionState()
  {
    final CompactionState compactionState = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
        ImmutableList.of(new CountAggregatorFactory("count")),
        new CompactionTransformSpec(new SelectorDimFilter("dim1", "foo", null)),
        MAPPER.convertValue(Map.of("test", "map"), IndexSpec.class),
        MAPPER.convertValue(Map.of("test2", "map2"), GranularitySpec.class),
        null
    );
    final DataSegment segment1 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .lastCompactionState(compactionState)
                                            .build();
    final DataSegment segment2 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .build();
    Assert.assertEquals(segment1, segment2.withLastCompactionState(compactionState));
  }

  @Test
  public void testAnnotateWithLastCompactionState()
  {
    DynamicPartitionsSpec dynamicPartitionsSpec = new DynamicPartitionsSpec(null, null);
    DimensionsSpec dimensionsSpec = new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of(
        "bar",
        "foo"
    )));
    List<AggregatorFactory> metricsSpec = ImmutableList.of(new CountAggregatorFactory("count"));
    CompactionTransformSpec transformSpec = new CompactionTransformSpec(
        new SelectorDimFilter("dim1", "foo", null)
    );
    IndexSpec indexSpec = MAPPER.convertValue(Map.of("test", "map"), IndexSpec.class);
    GranularitySpec granularitySpec = MAPPER.convertValue(Map.of("test2", "map"), GranularitySpec.class);

    final CompactionState compactionState = new CompactionState(
        dynamicPartitionsSpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        indexSpec,
        granularitySpec,
        null
    );

    final Function<Set<DataSegment>, Set<DataSegment>> addCompactionStateFunction =
        CompactionState.addCompactionStateToSegments(
            dynamicPartitionsSpec,
            dimensionsSpec,
            metricsSpec,
            transformSpec,
            indexSpec,
            granularitySpec,
            ImmutableList.of()
        );

    final DataSegment segment1 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .lastCompactionState(compactionState)
                                            .build();
    final DataSegment segment2 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .build();
    Assert.assertEquals(ImmutableSet.of(segment1), addCompactionStateFunction.apply(ImmutableSet.of(segment2)));
  }

  @Test
  public void testTombstoneType()
  {

    final DataSegment segment1 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(new TombstoneShardSpec())
                                            .loadSpec(Map.of(
                                                "type",
                                                DataSegment.TOMBSTONE_LOADSPEC_TYPE
                                            ))
                                            .size(0)
                                            .build();
    Assert.assertTrue(segment1.isTombstone());
    Assert.assertFalse(segment1.hasData());

    final DataSegment segment2 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .loadSpec(Map.of(
                                                "type",
                                                "foo"
                                            ))
                                            .size(0)
                                            .build();

    Assert.assertFalse(segment2.isTombstone());
    Assert.assertTrue(segment2.hasData());

    final DataSegment segment3 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .build();

    Assert.assertFalse(segment3.isTombstone());
    Assert.assertTrue(segment3.hasData());

  }

  @Test
  public void testSerializationWithCompactionStateFingerprint() throws Exception
  {
    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");
    final String fingerprint = "abc123def456";
    final SegmentId segmentId = SegmentId.of("something", interval, "1", new NumberedShardSpec(3, 0));

    DataSegment segment = DataSegment.builder(segmentId)
                                     .loadSpec(loadSpec)
                                     .dimensions(Arrays.asList("dim1", "dim2"))
                                     .metrics(Arrays.asList("met1", "met2"))
                                     .compactionStateFingerprint(fingerprint)
                                     .binaryVersion(TEST_VERSION)
                                     .size(1)
                                     .build();

    // Verify fingerprint is present in serialized JSON
    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segment),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertEquals(fingerprint, objectMap.get("compactionStateFingerprint"));

    // Verify deserialization preserves fingerprint
    DataSegment deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertEquals(fingerprint, deserializedSegment.getCompactionStateFingerprint());
    Assert.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testSerializationWithNullCompactionStateFingerprint() throws Exception
  {
    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");
    final SegmentId segmentId = SegmentId.of("something", interval, "1", new NumberedShardSpec(3, 0));

    DataSegment segment = DataSegment.builder(segmentId)
                                     .loadSpec(loadSpec)
                                     .dimensions(Arrays.asList("dim1", "dim2"))
                                     .metrics(Arrays.asList("met1", "met2"))
                                     .compactionStateFingerprint(null)
                                     .binaryVersion(TEST_VERSION)
                                     .size(1)
                                     .build();

    // Verify fingerprint is NOT present in serialized JSON (due to @JsonInclude(NON_NULL))
    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segment),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertFalse("compactionStateFingerprint should not be in JSON when null",
                       objectMap.containsKey("compactionStateFingerprint"));

    // Verify deserialization handles missing fingerprint
    DataSegment deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assert.assertNull(deserializedSegment.getCompactionStateFingerprint());
    Assert.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testDeserializationBackwardCompatibility_missingCompactionStateFingerprint() throws Exception
  {
    // Simulate JSON from old Druid version without compactionStateFingerprint field
    String jsonWithoutFingerprint = "{"
                                    + "\"dataSource\": \"something\","
                                    + "\"interval\": \"2011-10-01T00:00:00.000Z/2011-10-02T00:00:00.000Z\","
                                    + "\"version\": \"1\","
                                    + "\"loadSpec\": {\"something\": \"or_other\"},"
                                    + "\"dimensions\": \"dim1,dim2\","
                                    + "\"metrics\": \"met1,met2\","
                                    + "\"shardSpec\": {\"type\": \"numbered\", \"partitionNum\": 3, \"partitions\": 0},"
                                    + "\"binaryVersion\": 9,"
                                    + "\"size\": 1"
                                    + "}";

    DataSegment deserializedSegment = MAPPER.readValue(jsonWithoutFingerprint, DataSegment.class);
    Assert.assertNull("compactionStateFingerprint should be null for backward compatibility",
                      deserializedSegment.getCompactionStateFingerprint());
    Assert.assertEquals("something", deserializedSegment.getDataSource());
    Assert.assertEquals(Intervals.of("2011-10-01/2011-10-02"), deserializedSegment.getInterval());
  }

  @Test
  public void testWithCompactionStateFingerprint()
  {
    final String fingerprint = "test_fingerprint_12345";
    final Interval interval = Intervals.of("2012-01-01/2012-01-02");
    final String version = DateTimes.of("2012-01-01T11:22:33.444Z").toString();
    final ShardSpec shardSpec = getShardSpec(7);
    final SegmentId segmentId = SegmentId.of("foo", interval, version, shardSpec);

    final DataSegment segment1 = DataSegment.builder(segmentId)
                                            .size(0)
                                            .compactionStateFingerprint(fingerprint)
                                            .build();
    final DataSegment segment2 = DataSegment.builder(segmentId)
                                            .size(0)
                                            .build();

    DataSegment withFingerprint = segment2.withCompactionStateFingerprint(fingerprint);
    Assert.assertEquals(fingerprint, withFingerprint.getCompactionStateFingerprint());
    Assert.assertEquals(segment1, withFingerprint);
  }

  private static void assertAllFieldsEquals(DataSegment segment1, DataSegment segment2)
  {
    Assert.assertEquals(segment1.getDataSource(), segment2.getDataSource());
    Assert.assertEquals(segment1.getInterval(), segment2.getInterval());
    Assert.assertEquals(segment1.getVersion(), segment2.getVersion());
    Assert.assertEquals(segment1.getLoadSpec(), segment2.getLoadSpec());
    Assert.assertEquals(segment1.getDimensions(), segment2.getDimensions());
    Assert.assertEquals(segment1.getMetrics(), segment2.getMetrics());
    Assert.assertEquals(segment1.getProjections(), segment2.getProjections());
    Assert.assertEquals(segment1.getShardSpec(), segment2.getShardSpec());
    Assert.assertEquals(segment1.getSize(), segment2.getSize());
    Assert.assertEquals(segment1.getBinaryVersion(), segment2.getBinaryVersion());
    Assert.assertEquals(segment1.getId(), segment2.getId());
    Assert.assertEquals(segment1.getLastCompactionState(), segment2.getLastCompactionState());
  }
}
