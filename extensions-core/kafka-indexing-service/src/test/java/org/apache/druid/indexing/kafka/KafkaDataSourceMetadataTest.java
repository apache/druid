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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.utils.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class KafkaDataSourceMetadataTest
{
  private static final KafkaDataSourceMetadata START0 = startMetadata("foo", ImmutableMap.of());
  private static final KafkaDataSourceMetadata START1 = startMetadata("foo", ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata START2 = startMetadata("foo", ImmutableMap.of(0, 2L, 1, 4L, 2, 5L));
  private static final KafkaDataSourceMetadata START3 = startMetadata("foo", ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata START4 = startMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of());
  private static final KafkaDataSourceMetadata START5 = startMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata START6 = startMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata START7 = startMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata START8 = startMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata START9 = startMetadataMultiTopic("foo2.*", ImmutableList.of("foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END0 = endMetadata("foo", ImmutableMap.of());
  private static final KafkaDataSourceMetadata END1 = endMetadata("foo", ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END2 = endMetadata("foo", ImmutableMap.of(0, 2L, 1, 4L));
  private static final KafkaDataSourceMetadata END3 = endMetadata("foo", ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata END4 = endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of());
  private static final KafkaDataSourceMetadata END5 = endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END6 = endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 1, 4L));
  private static final KafkaDataSourceMetadata END7 = endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END8 = endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 1, 4L));
  private static final KafkaDataSourceMetadata END9 = endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END10 = endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L));

  @Test
  public void testMatches()
  {
    Assert.assertTrue(START0.matches(START0));
    Assert.assertTrue(START0.matches(START1));
    Assert.assertTrue(START0.matches(START2));
    Assert.assertTrue(START0.matches(START3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior
    Assert.assertFalse(START0.matches(START4));
    Assert.assertTrue(START0.matches(START5));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(START0.matches(START6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(START0.matches(START7));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(START0.matches(START8));
    // when merging, we lose the sequence numbers for topics foo2, and foo22 here when merging, so false
    Assert.assertFalse(START0.matches(START9));

    Assert.assertTrue(START1.matches(START0));
    Assert.assertTrue(START1.matches(START1));
    // sequence numbers for topic foo partition 1 are inconsistent, so false
    Assert.assertFalse(START1.matches(START2));
    Assert.assertTrue(START1.matches(START3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertTrue(START1.matches(START5));
    // when merging, we lose the sequence numbers for topic foo2, and sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(START1.matches(START6));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(START1.matches(START7));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(START1.matches(START8));
    // when merging, we lose the sequence numbers for topics foo2 and foo22, so false
    Assert.assertFalse(START1.matches(START9));

    Assert.assertTrue(START2.matches(START0));
    Assert.assertFalse(START2.matches(START1));
    Assert.assertTrue(START2.matches(START2));
    Assert.assertTrue(START2.matches(START3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START2.matches(START4));
    // when merging, sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(START2.matches(START5));
    // when merging, we lose the sequence numbers for topic foo2, and sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(START2.matches(START6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(START2.matches(START7));

    Assert.assertTrue(START3.matches(START0));
    Assert.assertTrue(START3.matches(START1));
    Assert.assertTrue(START3.matches(START2));
    Assert.assertTrue(START3.matches(START3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START3.matches(START4));
    Assert.assertTrue(START3.matches(START5));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(START3.matches(START6));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(START3.matches(START7));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(START3.matches(START8));
    // when merging, we lose the sequence numbers for topics foo2 and foo22, so false
    Assert.assertFalse(START3.matches(START9));

    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START0));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START1));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START2));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START3));
    Assert.assertTrue(START4.matches(START4));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START5));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START6));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START7));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START8));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(START4.matches(START9));

    Assert.assertTrue(START5.matches(START0));
    Assert.assertTrue(START5.matches(START1));
    // when merging, the sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(START5.matches(START2));
    Assert.assertTrue(START5.matches(START3));
    Assert.assertTrue(START5.matches(START4));
    Assert.assertTrue(START5.matches(START5));
    Assert.assertTrue(START5.matches(START6));
    Assert.assertTrue(START5.matches(START7));
    Assert.assertTrue(START5.matches(START8));
    Assert.assertTrue(START5.matches(START9));

    Assert.assertTrue(START6.matches(START0));
    Assert.assertTrue(START6.matches(START1));
    // when merging, the sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(START6.matches(START2));
    Assert.assertTrue(START6.matches(START3));
    Assert.assertTrue(START6.matches(START4));
    Assert.assertTrue(START6.matches(START5));
    Assert.assertTrue(START6.matches(START6));
    Assert.assertTrue(START6.matches(START7));
    Assert.assertTrue(START6.matches(START8));
    Assert.assertTrue(START6.matches(START9));

    Assert.assertTrue(START7.matches(START0));
    Assert.assertTrue(START7.matches(START1));
    Assert.assertTrue(START7.matches(START2));
    Assert.assertTrue(START7.matches(START3));
    Assert.assertTrue(START7.matches(START4));
    Assert.assertTrue(START7.matches(START5));
    Assert.assertTrue(START7.matches(START6));
    Assert.assertTrue(START7.matches(START7));
    Assert.assertTrue(START7.matches(START8));
    Assert.assertTrue(START7.matches(START9));

    Assert.assertTrue(START8.matches(START0));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START8.matches(START1));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START8.matches(START2));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START8.matches(START3));
    Assert.assertTrue(START8.matches(START4));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START8.matches(START5));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START8.matches(START6));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START8.matches(START7));
    Assert.assertTrue(START8.matches(START8));
    Assert.assertTrue(START8.matches(START9));

    Assert.assertTrue(START9.matches(START0));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START9.matches(START1));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START9.matches(START2));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START9.matches(START3));
    Assert.assertTrue(START9.matches(START4));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START9.matches(START5));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START9.matches(START6));
    // when merging, we lose the sequence numbers for topic foo, so false
    Assert.assertFalse(START9.matches(START7));
    Assert.assertTrue(START9.matches(START8));
    Assert.assertTrue(START9.matches(START9));

    Assert.assertTrue(END0.matches(END0));
    Assert.assertTrue(END0.matches(END1));
    Assert.assertTrue(END0.matches(END2));
    Assert.assertTrue(END0.matches(END3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END0.matches(END4));
    Assert.assertTrue(END0.matches(END5));
    Assert.assertTrue(END0.matches(END6));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(END0.matches(END7));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(END0.matches(END8));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(END0.matches(END9));
    // when merging, we lose the sequence numbers for topic foo2, so false
    Assert.assertFalse(END0.matches(END10));

    Assert.assertTrue(END1.matches(END0));
    Assert.assertTrue(END1.matches(END1));
    Assert.assertTrue(END1.matches(END2));
    Assert.assertTrue(END1.matches(END3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END1.matches(END4));
    Assert.assertTrue(END1.matches(END5));
    Assert.assertTrue(END1.matches(END6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END1.matches(END7));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END1.matches(END8));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END1.matches(END9));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END1.matches(END10));

    Assert.assertTrue(END2.matches(END0));
    Assert.assertTrue(END2.matches(END1));
    Assert.assertTrue(END2.matches(END2));
    // when merging, the sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(END2.matches(END3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END2.matches(END4));
    Assert.assertTrue(END2.matches(END5));
    Assert.assertTrue(END2.matches(END6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END2.matches(END7));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END2.matches(END8));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END2.matches(END9));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END2.matches(END10));

    Assert.assertTrue(END3.matches(END0));
    Assert.assertTrue(END3.matches(END1));
    // when merging, the sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(END3.matches(END2));
    Assert.assertTrue(END3.matches(END3));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END3.matches(END4));
    Assert.assertTrue(END3.matches(END5));
    // when merging, the sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(END3.matches(END6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END3.matches(END7));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END3.matches(END8));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END3.matches(END9));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END3.matches(END10));

    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END0));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END1));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END2));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END3));
    Assert.assertTrue(END4.matches(END4));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END5));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END6));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END7));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END8));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END9));
    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertFalse(END4.matches(END10));

    Assert.assertTrue(END5.matches(END0));
    Assert.assertTrue(END5.matches(END1));
    Assert.assertTrue(END5.matches(END2));
    Assert.assertTrue(END5.matches(END3));
    Assert.assertTrue(END5.matches(END4));
    Assert.assertTrue(END5.matches(END5));
    Assert.assertTrue(END5.matches(END6));
    Assert.assertTrue(END5.matches(END7));
    Assert.assertTrue(END5.matches(END8));
    Assert.assertTrue(END5.matches(END9));
    Assert.assertTrue(END5.matches(END10));

    Assert.assertTrue(END6.matches(END0));
    Assert.assertTrue(END6.matches(END1));
    Assert.assertTrue(END6.matches(END2));
    // when merging, the sequence numbers for topic foo-1 are inconsistent, so false
    Assert.assertFalse(END6.matches(END3));
    Assert.assertTrue(END6.matches(END4));
    Assert.assertTrue(END6.matches(END5));
    Assert.assertTrue(END6.matches(END6));
    Assert.assertTrue(END6.matches(END7));
    Assert.assertTrue(END6.matches(END8));
    Assert.assertTrue(END6.matches(END9));
    Assert.assertTrue(END6.matches(END10));

    Assert.assertTrue(END7.matches(END0));
    Assert.assertTrue(END7.matches(END1));
    Assert.assertTrue(END7.matches(END2));
    Assert.assertTrue(END7.matches(END3));
    Assert.assertTrue(END7.matches(END4));
    Assert.assertTrue(END7.matches(END5));
    Assert.assertTrue(END7.matches(END6));
    Assert.assertTrue(END7.matches(END7));
    Assert.assertTrue(END7.matches(END8));
    Assert.assertTrue(END7.matches(END9));
    Assert.assertTrue(END7.matches(END10));

    Assert.assertTrue(END8.matches(END0));
    Assert.assertTrue(END8.matches(END1));
    Assert.assertTrue(END8.matches(END2));
    // when merging, the sequence numbers for topic foo-1, and foo-2 are inconsistent, so false
    Assert.assertFalse(END8.matches(END3));
    Assert.assertTrue(END8.matches(END4));
    Assert.assertTrue(END8.matches(END5));
    Assert.assertTrue(END8.matches(END6));
    Assert.assertTrue(END8.matches(END7));
    Assert.assertTrue(END8.matches(END8));
    Assert.assertTrue(END8.matches(END9));
    Assert.assertTrue(END8.matches(END10));

    Assert.assertTrue(END9.matches(END0));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END1));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END2));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END3));
    Assert.assertTrue(END9.matches(END4));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END5));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END7));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END9.matches(END8));
    Assert.assertTrue(END9.matches(END9));
    Assert.assertTrue(END9.matches(END10));

    Assert.assertTrue(END10.matches(END0));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END1));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END2));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END3));
    Assert.assertTrue(END10.matches(END4));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END5));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END6));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END7));
    // when merging, we lose the sequence numbers for topic foo2 here when merging, so false
    Assert.assertFalse(END10.matches(END8));
    Assert.assertTrue(END10.matches(END9));
    Assert.assertTrue(END10.matches(END10));
  }

  @Test
  public void testIsValidStart()
  {
    Assert.assertTrue(START0.isValidStart());
    Assert.assertTrue(START1.isValidStart());
    Assert.assertTrue(START2.isValidStart());
    Assert.assertTrue(START3.isValidStart());
    Assert.assertTrue(START4.isValidStart());
    Assert.assertTrue(START5.isValidStart());
    Assert.assertTrue(START6.isValidStart());
    Assert.assertTrue(START7.isValidStart());
    Assert.assertTrue(START8.isValidStart());
    Assert.assertTrue(START9.isValidStart());
  }

  @Test
  public void testPlus()
  {
    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        START1.plus(START3)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        START0.plus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        START1.plus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        START2.plus(START1)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        START2.plus(START2)
    );

    // add comment on this
    Assert.assertEquals(
        START4,
        START1.plus(START4)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L)),
        START1.plus(START5)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L)),
        START1.plus(START6)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        START1.plus(START7)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        START2.plus(START6)
    );

    // add comment on this
    Assert.assertEquals(
        START0,
        START4.plus(START0)
    );

    // add comment on this
    Assert.assertEquals(
        START1,
        START4.plus(START1)
    );

    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertEquals(
        START4,
        START4.plus(START5)
    );

    Assert.assertEquals(
        START5,
        START5.plus(START4)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 1, 3L)),
        START5.plus(START6)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        START7.plus(START8)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        START7.plus(START9)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        START8.plus(START7)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo2.*", ImmutableList.of("foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        START8.plus(START9)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo2.*", ImmutableList.of("foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        START9.plus(START8)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(0, 2L, 2, 5L)),
        END0.plus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        END1.plus(END2)
    );

    // add comment on this
    Assert.assertEquals(
        END4,
        END0.plus(END4)
    );

    // add comment on this
    Assert.assertEquals(
        END4,
        END1.plus(END4)
    );

    // add comment on this
    Assert.assertEquals(
        END0,
        END4.plus(END0)
    );

    // add comment on this
    Assert.assertEquals(
        END1,
        END4.plus(END1)
    );

    Assert.assertEquals(
        END3,
        END2.plus(END3)
    );

    Assert.assertEquals(
        END2,
        END3.plus(END2)
    );

    // when sequence map is empty, we don't know whether its multi-topic or not, so assume single topic to preserve existing behavior    Assert.assertFalse(START1.matches(START4));
    Assert.assertEquals(
        END4,
        END4.plus(END5)
    );

    Assert.assertEquals(
        END5,
        END5.plus(END4)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        END5.plus(END9)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        END5.plus(END10)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        END5.plus(END6)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        END5.plus(END7)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        END7.plus(END5)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        END9.plus(END5)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        END9.plus(END10)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2", "foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        END10.plus(END9)
    );
  }

  @Test
  public void testMinus()
  {
    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START0.minus(START2)
    );

    Assert.assertEquals(
        START0,
        START0.minus(START4)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START1.minus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(1, 3L)),
        START1.minus(START3)
    );

    Assert.assertEquals(
        START1,
        START1.minus(START4)
    );

    Assert.assertEquals(
        startMetadata("foo", ImmutableMap.of()),
        START1.minus(START5)
    );

    Assert.assertEquals(
        startMetadata("foo", ImmutableMap.of()),
        START1.minus(START6)
    );

    Assert.assertEquals(
        startMetadata("foo", ImmutableMap.of(1, 3L)),
        START1.minus(START7)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of(2, 5L)),
        START2.minus(START1)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START2.minus(START2)
    );

    Assert.assertEquals(
        START4,
        START4.minus(START0)
    );

    Assert.assertEquals(
        START4,
        START4.minus(START1)
    );

    Assert.assertEquals(
        startMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of()),
        START5.minus(START1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(1, 4L)),
        END2.minus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(2, 5L)),
        END1.minus(END2)
    );

    Assert.assertEquals(
        END0,
        END0.minus(END4)
    );

    Assert.assertEquals(
        END4,
        END4.minus(END0)
    );

    Assert.assertEquals(
        END1,
        END1.minus(END4)
    );

    Assert.assertEquals(
        END4,
        END4.minus(END1)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of()),
        END5.minus(END1)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 2, 5L)),
        END5.minus(END4)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(2, 5L)),
        END5.minus(END6)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(1, 4L)),
        END6.minus(END5)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(1, 4L)),
        END6.minus(END7)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo2"), ImmutableMap.of(0, 2L, 2, 5L)),
        END7.minus(END5)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo", "foo2"), ImmutableMap.of(2, 5L)),
        END7.minus(END8)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 2, 5L)),
        END7.minus(END9)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo.*", ImmutableList.of("foo"), ImmutableMap.of(0, 2L, 2, 5L)),
        END7.minus(END10)
    );

    Assert.assertEquals(
        END9,
        END9.minus(END6)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of()),
        END9.minus(END7)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of(2, 5L)),
        END9.minus(END8)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of()),
        END9.minus(END9)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo2"), ImmutableMap.of()),
        END9.minus(END10)
    );

    Assert.assertEquals(
        endMetadataMultiTopic("foo2.*", ImmutableList.of("foo22"), ImmutableMap.of(0, 2L, 2, 5L)),
        END10.minus(END9)
    );
  }

  @Test
  public void testKafkaDataSourceMetadataSerdeRoundTrip() throws JsonProcessingException
  {
    ObjectMapper jsonMapper = createObjectMapper();

    KafkaDataSourceMetadata kdm1 = endMetadata(ImmutableMap.of());
    String kdmStr1 = jsonMapper.writeValueAsString(kdm1);
    DataSourceMetadata dsMeta1 = jsonMapper.readValue(kdmStr1, DataSourceMetadata.class);
    Assert.assertEquals(kdm1, dsMeta1);

    KafkaDataSourceMetadata kdm2 = endMetadata(ImmutableMap.of(1, 3L));
    String kdmStr2 = jsonMapper.writeValueAsString(kdm2);
    DataSourceMetadata dsMeta2 = jsonMapper.readValue(kdmStr2, DataSourceMetadata.class);
    Assert.assertEquals(kdm2, dsMeta2);
  }

  @Test
  public void testKafkaDataSourceMetadataSerde() throws JsonProcessingException
  {
    ObjectMapper jsonMapper = createObjectMapper();
    KafkaDataSourceMetadata expectedKdm1 = endMetadata(ImmutableMap.of(1, 3L));
    String kdmStr1 = "{\"type\":\"kafka\",\"partitions\":{\"type\":\"end\",\"stream\":\"foo\",\"topic\":\"foo\",\"partitionSequenceNumberMap\":{\"1\":3},\"partitionOffsetMap\":{\"1\":3},\"exclusivePartitions\":[]}}\n";
    DataSourceMetadata dsMeta1 = jsonMapper.readValue(kdmStr1, DataSourceMetadata.class);
    Assert.assertEquals(dsMeta1, expectedKdm1);

    KafkaDataSourceMetadata expectedKdm2 = endMetadata(ImmutableMap.of(1, 3L, 2, 1900L));
    String kdmStr2 = "{\"type\":\"kafka\",\"partitions\":{\"type\":\"end\",\"stream\":\"foo\",\"topic\":\"food\",\"partitionSequenceNumberMap\":{\"1\":3, \"2\":1900},\"partitionOffsetMap\":{\"1\":3, \"2\":1900},\"exclusivePartitions\":[]}}\n";
    DataSourceMetadata dsMeta2 = jsonMapper.readValue(kdmStr2, DataSourceMetadata.class);
    Assert.assertEquals(dsMeta2, expectedKdm2);
  }

  private static KafkaDataSourceMetadata startMetadata(Map<Integer, Long> offsets)
  {
    return startMetadata("foo", offsets);
  }

  private static KafkaDataSourceMetadata startMetadata(
      String topic,
      Map<Integer, Long> offsets
  )
  {
    Map<KafkaTopicPartition, Long> newOffsets = CollectionUtils.mapKeys(
        offsets,
        k -> new KafkaTopicPartition(
            false,
            topic,
            k
        )
    );
    return new KafkaDataSourceMetadata(new SeekableStreamStartSequenceNumbers<>(topic, newOffsets, ImmutableSet.of()));
  }

  private static KafkaDataSourceMetadata startMetadataMultiTopic(
      String topicPattern,
      List<String> topics,
      Map<Integer, Long> offsets
  )
  {
    Assert.assertFalse(topics.isEmpty());
    Pattern pattern = Pattern.compile(topicPattern);
    Assert.assertTrue(topics.stream().allMatch(t -> pattern.matcher(t).matches()));
    Map<KafkaTopicPartition, Long> newOffsets = new HashMap<>();
    for (Map.Entry<Integer, Long> e : offsets.entrySet()) {
      for (String topic : topics) {
        newOffsets.put(
            new KafkaTopicPartition(
                true,
                topic,
                e.getKey()

            ),
            e.getValue()
        );
      }
    }
    return new KafkaDataSourceMetadata(new SeekableStreamStartSequenceNumbers<>(topicPattern, newOffsets, ImmutableSet.of()));
  }


  private static KafkaDataSourceMetadata endMetadata(Map<Integer, Long> offsets)
  {
    return endMetadata("foo", offsets);
  }

  private static KafkaDataSourceMetadata endMetadata(String topic, Map<Integer, Long> offsets)
  {
    Map<KafkaTopicPartition, Long> newOffsets = CollectionUtils.mapKeys(
        offsets,
        k -> new KafkaTopicPartition(
            false,
            "foo",
            k
        )
    );
    return new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, newOffsets));
  }

  private static KafkaDataSourceMetadata endMetadataMultiTopic(
      String topicPattern,
      List<String> topics,
      Map<Integer, Long> offsets
  )
  {
    Assert.assertFalse(topics.isEmpty());
    Pattern pattern = Pattern.compile(topicPattern);
    Assert.assertTrue(topics.stream().allMatch(t -> pattern.matcher(t).matches()));
    Map<KafkaTopicPartition, Long> newOffsets = new HashMap<>();
    for (Map.Entry<Integer, Long> e : offsets.entrySet()) {
      for (String topic : topics) {
        newOffsets.put(
            new KafkaTopicPartition(
                true,
                topic,
                e.getKey()

            ),
            e.getValue()
        );
      }
    }
    return new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topicPattern, newOffsets));
  }

  private static ObjectMapper createObjectMapper()
  {
    DruidModule module = new KafkaIndexTaskModule();
    final Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build())
        .addModule(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8000);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(9000);
            }
        ).build();

    ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
    module.getJacksonModules().forEach(objectMapper::registerModule);
    return objectMapper;
  }
}
