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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.Map;

public class SeekableStreamStartSequenceNumbersTest
{
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    final String stream = "theStream";
    final Map<Integer, Long> offsetMap = ImmutableMap.of(1, 2L, 3, 4L);

    final SeekableStreamStartSequenceNumbers<Integer, Long> partitions = new SeekableStreamStartSequenceNumbers<>(
        stream,
        offsetMap,
        ImmutableSet.of(6)
    );
    final String serializedString = OBJECT_MAPPER.writeValueAsString(partitions);

    // Check round-trip.
    final SeekableStreamStartSequenceNumbers<Integer, Long> partitions2 = OBJECT_MAPPER.readValue(
        serializedString,
        new TypeReference<SeekableStreamStartSequenceNumbers<Integer, Long>>() {}
    );

    Assert.assertEquals("Round trip", partitions, partitions2);

    // Check backwards compatibility.
    final Map<String, Object> asMap = OBJECT_MAPPER.readValue(
        serializedString,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(stream, asMap.get("stream"));
    Assert.assertEquals(stream, asMap.get("topic"));

    // Jackson will deserialize the maps as string -> int maps, not int -> long.
    Assert.assertEquals(
        offsetMap,
        OBJECT_MAPPER.convertValue(asMap.get("partitionSequenceNumberMap"), new TypeReference<Map<Integer, Long>>() {})
    );
    Assert.assertEquals(
        offsetMap,
        OBJECT_MAPPER.convertValue(asMap.get("partitionOffsetMap"), new TypeReference<Map<Integer, Long>>() {})
    );
  }

  @Test
  public void testCompareToWithTrueResult()
  {
    final String stream = "theStream";
    final Map<Integer, Long> offsetMap1 = ImmutableMap.of(1, 5L, 2, 6L);
    final SeekableStreamStartSequenceNumbers<Integer, Long> partitions1 = new SeekableStreamStartSequenceNumbers<>(
        stream,
        offsetMap1,
        ImmutableSet.of(6)
    );

    final Map<Integer, Long> offsetMap2 = ImmutableMap.of(1, 4L, 2, 4L);
    final SeekableStreamStartSequenceNumbers<Integer, Long> partitions2 = new SeekableStreamStartSequenceNumbers<>(
        stream,
        offsetMap2,
        ImmutableSet.of(6)
    );
    Assert.assertEquals(1, partitions1.compareTo(partitions2, Comparator.naturalOrder()));
  }

  @Test
  public void testCompareToWithFalseResult()
  {
    final String stream = "theStream";
    final Map<Integer, Long> offsetMap1 = ImmutableMap.of(1, 3L, 2, 2L);
    final SeekableStreamStartSequenceNumbers<Integer, Long> partitions1 = new SeekableStreamStartSequenceNumbers<>(
        stream,
        offsetMap1,
        ImmutableSet.of(6)
    );

    final Map<Integer, Long> offsetMap2 = ImmutableMap.of(1, 4L, 2, 4L);
    final SeekableStreamStartSequenceNumbers<Integer, Long> partitions2 = new SeekableStreamStartSequenceNumbers<>(
        stream,
        offsetMap2,
        ImmutableSet.of(6)
    );
    Assert.assertEquals(0, partitions1.compareTo(partitions2, Comparator.naturalOrder()));
  }
}
