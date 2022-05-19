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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.TestObjectMapper;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SegmentWithOvershadowedStatusTest
{
  private static final ObjectMapper MAPPER = createObjectMapper();
  private static final Interval INTERVAL = Intervals.of("2011-10-01/2011-10-02");
  private static final ImmutableMap<String, Object> LOAD_SPEC = ImmutableMap.of("something", "or_other");
  private static final boolean OVERSHADOWED = true;
  private static final int TEST_VERSION = 0x9;
  private static final SegmentWithOvershadowedStatus SEGMENT = createSegmentWithOvershadowedStatus();

  private static ObjectMapper createObjectMapper()
  {
    ObjectMapper objectMapper = new TestObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    objectMapper.setInjectableValues(injectableValues);
    return objectMapper;
  }

  private static SegmentWithOvershadowedStatus createSegmentWithOvershadowedStatus()
  {
    DataSegment dataSegment = new DataSegment(
        "something",
        INTERVAL,
        "1",
        LOAD_SPEC,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        NoneShardSpec.instance(),
        null,
        TEST_VERSION,
        1
    );

    return new SegmentWithOvershadowedStatus(dataSegment, OVERSHADOWED);
  }

  @Test
  public void testUnwrappedSegmentWithOvershadowedStatusDeserialization() throws Exception
  {
    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(SEGMENT),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(11, objectMap.size());
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(INTERVAL.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(LOAD_SPEC, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assert.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
    Assert.assertEquals(OVERSHADOWED, objectMap.get("overshadowed"));

    final String json = MAPPER.writeValueAsString(SEGMENT);

    final TestSegmentWithOvershadowedStatus deserializedSegment = MAPPER.readValue(
        json,
        TestSegmentWithOvershadowedStatus.class
    );

    DataSegment dataSegment = SEGMENT.getDataSegment();
    Assert.assertEquals(dataSegment.getDataSource(), deserializedSegment.getDataSource());
    Assert.assertEquals(dataSegment.getInterval(), deserializedSegment.getInterval());
    Assert.assertEquals(dataSegment.getVersion(), deserializedSegment.getVersion());
    Assert.assertEquals(dataSegment.getLoadSpec(), deserializedSegment.getLoadSpec());
    Assert.assertEquals(dataSegment.getDimensions(), deserializedSegment.getDimensions());
    Assert.assertEquals(dataSegment.getMetrics(), deserializedSegment.getMetrics());
    Assert.assertEquals(dataSegment.getShardSpec(), deserializedSegment.getShardSpec());
    Assert.assertEquals(dataSegment.getSize(), deserializedSegment.getSize());
    Assert.assertEquals(dataSegment.getId(), deserializedSegment.getId());
  }

  // Previously, the implementation of SegmentWithOvershadowedStatus had @JsonCreator/@JsonProperty and @JsonUnwrapped
  // on the same field (dataSegment), which used to work in Jackson 2.6, but does not work with Jackson 2.9:
  // https://github.com/FasterXML/jackson-databind/issues/265#issuecomment-264344051
  @Test
  public void testJsonCreatorAndJsonUnwrappedAnnotationsAreCompatible() throws Exception
  {
    String json = MAPPER.writeValueAsString(SEGMENT);
    SegmentWithOvershadowedStatus segment = MAPPER.readValue(json, SegmentWithOvershadowedStatus.class);
    Assert.assertEquals(SEGMENT, segment);
    Assert.assertEquals(json, MAPPER.writeValueAsString(segment));
  }
}

/**
 * Subclass of DataSegment with overshadowed status
 */
class TestSegmentWithOvershadowedStatus extends DataSegment
{
  private final boolean overshadowed;

  @JsonCreator
  public TestSegmentWithOvershadowedStatus(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("version") String version,
      @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
      @JsonProperty("dimensions")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
          List<String> dimensions,
      @JsonProperty("metrics")
      @JsonDeserialize(using = CommaListJoinDeserializer.class)
      @Nullable
          List<String> metrics,
      @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
      @JsonProperty("lasCompactionState") @Nullable CompactionState lastCompactionState,
      @JsonProperty("binaryVersion") Integer binaryVersion,
      @JsonProperty("size") long size,
      @JsonProperty("overshadowed") boolean overshadowed
  )
  {
    super(
        dataSource,
        interval,
        version,
        loadSpec,
        dimensions,
        metrics,
        shardSpec,
        lastCompactionState,
        binaryVersion,
        size
    );
    this.overshadowed = overshadowed;
  }

  @JsonProperty
  public boolean isOvershadowed()
  {
    return overshadowed;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TestSegmentWithOvershadowedStatus)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final TestSegmentWithOvershadowedStatus that = (TestSegmentWithOvershadowedStatus) o;
    if (overshadowed != (that.overshadowed)) {
      return false;
    }
    return true;
  }

}
