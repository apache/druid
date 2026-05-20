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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class SegmentChangeRequestLoadTest
{
  private static DataSegment.Builder segmentBuilder(String dataSource, Interval interval, String version)
  {
    return DataSegment.builder(SegmentId.of(dataSource, interval, version, NoneShardSpec.instance()))
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(IndexIO.CURRENT_VERSION_ID);
  }

  @Test
  public void testV1Serialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = segmentBuilder("something", interval, "1")
        .loadSpec(loadSpec)
        .dimensions(Arrays.asList("dim1", "dim2"))
        .metrics(Arrays.asList("met1", "met2"))
        .size(1)
        .build();

    final SegmentChangeRequestLoad segmentDrop = new SegmentChangeRequestLoad(segment);

    Map<String, Object> objectMap = mapper.readValue(
        mapper.writeValueAsString(segmentDrop), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(11, objectMap.size());
    Assert.assertEquals("load", objectMap.get("action"));
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assert.assertEquals(IndexIO.CURRENT_VERSION_ID, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
  }

  @Test
  public void testPartialLoadFieldsOmittedWhenNull() throws Exception
  {
    // Coordinator-issued load requests leave the partial-load fields null; the wire payload should not include them.
    ObjectMapper mapper = new DefaultObjectMapper();
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(Map.of("type", "local"))
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(1)
        .build();
    SegmentChangeRequestLoad load = new SegmentChangeRequestLoad(segment);
    Map<String, Object> objectMap = mapper.readValue(
        mapper.writeValueAsString(load),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertFalse(objectMap.containsKey("fingerprint"));
    Assert.assertFalse(objectMap.containsKey("loadedBytes"));
  }

  @Test
  public void testPartialLoadFieldsRoundTrip() throws Exception
  {
    // Historical announcement with partial-load metadata: round-trip preserves both fields.
    ObjectMapper mapper = new DefaultObjectMapper();
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(Map.of("type", "local"))
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(1)
        .build();
    SegmentChangeRequestLoad load = new SegmentChangeRequestLoad(
        segment,
        "v1:abcdef0123456789",
        12345L
    );
    String json = mapper.writeValueAsString(load);
    SegmentChangeRequestLoad reread = mapper.readValue(json, SegmentChangeRequestLoad.class);
    Assert.assertEquals(load, reread);
    Assert.assertEquals("v1:abcdef0123456789", reread.getFingerprint());
    Assert.assertEquals(Long.valueOf(12345L), reread.getLoadedBytes());
  }

  @Test
  public void testForAnnouncementBareSegment()
  {
    // A segment loaded without a partial-load wrapper produces a plain announcement with no partial fields.
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(Map.of("type", "local"))
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(100)
        .build();
    SegmentChangeRequestLoad announcement = SegmentChangeRequestLoad.forAnnouncement(segment);
    Assert.assertNull(announcement.getFingerprint());
    Assert.assertNull(announcement.getLoadedBytes());
  }

  @Test
  public void testForAnnouncementPartialProjectionWrapperProducesFullFallback()
  {
    // When the segment's loadSpec is a partialProjection wrapper, the announcement stamps the wrapper's fingerprint
    // and the segment's full size as loadedBytes — coordinator reads this as a full-fallback profile and counts the
    // replica as matching, avoiding reload thrash on historicals that don't (yet) do real partial loading.
    Map<String, Object> wrapped = Map.of(
        "type", "partialProjection",
        "delegate", Map.of("type", "local", "path", "/var/druid/segments/foo"),
        "projections", List.of("revenue"),
        "fingerprint", "v1:abcdef0123456789"
    );
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(wrapped)
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(12345)
        .build();
    SegmentChangeRequestLoad announcement = SegmentChangeRequestLoad.forAnnouncement(segment);
    Assert.assertEquals("v1:abcdef0123456789", announcement.getFingerprint());
    Assert.assertEquals(Long.valueOf(12345L), announcement.getLoadedBytes());
  }

  @Test
  public void testForAnnouncementUnknownPartialTypeStillRecognizedByConvention()
  {
    // Any partial-load wire form (type starts with "partial", plus fingerprint + delegate at top level) gets the
    // full-fallback treatment — the announcement layer doesn't need to know about specific subtypes. This is what
    // makes future PartialLoadSpec subtypes work without touching this code.
    Map<String, Object> wrapped = Map.of(
        "type", "partialFutureScheme",
        "delegate", Map.of("type", "local", "path", "/var/druid/segments/foo"),
        "fingerprint", "v1:1111111111111111"
    );
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(wrapped)
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(7777)
        .build();
    SegmentChangeRequestLoad announcement = SegmentChangeRequestLoad.forAnnouncement(segment);
    Assert.assertEquals("v1:1111111111111111", announcement.getFingerprint());
    Assert.assertEquals(Long.valueOf(7777L), announcement.getLoadedBytes());
  }

  @Test
  public void testForAnnouncementNonPartialTypeIgnoredEvenWithFingerprint()
  {
    // The type-prefix gate keeps non-partial LoadSpecs that happen to use a "fingerprint" key from being
    // mis-classified as partial-load wrappers.
    Map<String, Object> looksSuspicious = Map.of(
        "type", "local",
        "path", "/var/druid/segments/foo",
        "fingerprint", "v1:notreallypartial",
        "delegate", Map.of("ignored", "yes")
    );
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(looksSuspicious)
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(100)
        .build();
    SegmentChangeRequestLoad announcement = SegmentChangeRequestLoad.forAnnouncement(segment);
    Assert.assertNull(announcement.getFingerprint());
    Assert.assertNull(announcement.getLoadedBytes());
  }

  @Test
  public void testForAnnouncementMalformedPartialTypeFallsThroughToPlainLoad()
  {
    // A partial-typed loadSpec missing the fingerprint contract should not stall the queue: announce as a plain
    // load (the log.warn at the call site surfaces the bug).
    Map<String, Object> malformed = Map.of(
        "type", "partialProjection",
        "delegate", Map.of("type", "local", "path", "/var/druid/segments/foo")
        // no fingerprint
    );
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(malformed)
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(100)
        .build();
    SegmentChangeRequestLoad announcement = SegmentChangeRequestLoad.forAnnouncement(segment);
    Assert.assertNull(announcement.getFingerprint());
    Assert.assertNull(announcement.getLoadedBytes());
  }

  @Test
  public void testOldPayloadDeserializesWithoutPartialFields() throws Exception
  {
    // An old-version payload with no partial-load fields should deserialize cleanly; the partial fields are null.
    ObjectMapper mapper = new DefaultObjectMapper();
    DataSegment segment = segmentBuilder("ds", Intervals.of("2024-01-01/2024-02-01"), "v1")
        .loadSpec(Map.of("type", "local"))
        .dimensions(List.of("d"))
        .metrics(List.of("m"))
        .size(1)
        .build();
    String oldJson = mapper.writeValueAsString(new SegmentChangeRequestLoad(segment));
    SegmentChangeRequestLoad reread = mapper.readValue(oldJson, SegmentChangeRequestLoad.class);
    Assert.assertNull(reread.getFingerprint());
    Assert.assertNull(reread.getLoadedBytes());
  }
}
