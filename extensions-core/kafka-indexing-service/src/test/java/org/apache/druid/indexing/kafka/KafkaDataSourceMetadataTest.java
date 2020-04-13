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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KafkaDataSourceMetadataTest
{
  private static final KafkaDataSourceMetadata START0 = startMetadata(ImmutableMap.of());
  private static final KafkaDataSourceMetadata START1 = startMetadata(ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata START2 = startMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L));
  private static final KafkaDataSourceMetadata START3 = startMetadata(ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END0 = endMetadata(ImmutableMap.of());
  private static final KafkaDataSourceMetadata END1 = endMetadata(ImmutableMap.of(0, 2L, 2, 5L));
  private static final KafkaDataSourceMetadata END2 = endMetadata(ImmutableMap.of(0, 2L, 1, 4L));

  @Test
  public void testMatches()
  {
    Assert.assertTrue(START0.matches(START0));
    Assert.assertTrue(START0.matches(START1));
    Assert.assertTrue(START0.matches(START2));
    Assert.assertTrue(START0.matches(START3));

    Assert.assertTrue(START1.matches(START0));
    Assert.assertTrue(START1.matches(START1));
    Assert.assertFalse(START1.matches(START2));
    Assert.assertTrue(START1.matches(START3));

    Assert.assertTrue(START2.matches(START0));
    Assert.assertFalse(START2.matches(START1));
    Assert.assertTrue(START2.matches(START2));
    Assert.assertTrue(START2.matches(START3));

    Assert.assertTrue(START3.matches(START0));
    Assert.assertTrue(START3.matches(START1));
    Assert.assertTrue(START3.matches(START2));
    Assert.assertTrue(START3.matches(START3));

    Assert.assertTrue(END0.matches(END0));
    Assert.assertTrue(END0.matches(END1));
    Assert.assertTrue(END0.matches(END2));

    Assert.assertTrue(END1.matches(END0));
    Assert.assertTrue(END1.matches(END1));
    Assert.assertTrue(END1.matches(END2));

    Assert.assertTrue(END2.matches(END0));
    Assert.assertTrue(END2.matches(END1));
    Assert.assertTrue(END2.matches(END2));
  }

  @Test
  public void testIsValidStart()
  {
    Assert.assertTrue(START0.isValidStart());
    Assert.assertTrue(START1.isValidStart());
    Assert.assertTrue(START2.isValidStart());
    Assert.assertTrue(START3.isValidStart());
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

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(0, 2L, 2, 5L)),
        END0.plus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        END1.plus(END2)
    );
  }

  @Test
  public void testMinus()
  {
    Assert.assertEquals(
        startMetadata(ImmutableMap.of(1, 3L)),
        START1.minus(START3)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START0.minus(START2)
    );

    Assert.assertEquals(
        startMetadata(ImmutableMap.of()),
        START1.minus(START2)
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
        endMetadata(ImmutableMap.of(1, 4L)),
        END2.minus(END1)
    );

    Assert.assertEquals(
        endMetadata(ImmutableMap.of(2, 5L)),
        END1.minus(END2)
    );
  }

  private static KafkaDataSourceMetadata startMetadata(Map<Integer, Long> offsets)
  {
    return new KafkaDataSourceMetadata(new SeekableStreamStartSequenceNumbers<>("foo", offsets, ImmutableSet.of()));
  }

  private static KafkaDataSourceMetadata endMetadata(Map<Integer, Long> offsets)
  {
    return new KafkaDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>("foo", offsets));
  }
}
