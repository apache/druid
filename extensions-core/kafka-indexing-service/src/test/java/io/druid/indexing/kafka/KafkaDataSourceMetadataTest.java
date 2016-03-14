/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class KafkaDataSourceMetadataTest
{
  private static final KafkaDataSourceMetadata KM0 = KM("foo", ImmutableMap.<Integer, Long>of());
  private static final KafkaDataSourceMetadata KM1 = KM("foo", ImmutableMap.of(0, 2L, 1, 3L));
  private static final KafkaDataSourceMetadata KM2 = KM("foo", ImmutableMap.of(0, 2L, 1, 4L, 2, 5L));
  private static final KafkaDataSourceMetadata KM3 = KM("foo", ImmutableMap.of(0, 2L, 2, 5L));

  @Test
  public void testMatches()
  {
    Assert.assertTrue(KM0.matches(KM0));
    Assert.assertTrue(KM0.matches(KM1));
    Assert.assertTrue(KM0.matches(KM2));
    Assert.assertTrue(KM0.matches(KM3));

    Assert.assertTrue(KM1.matches(KM0));
    Assert.assertTrue(KM1.matches(KM1));
    Assert.assertFalse(KM1.matches(KM2));
    Assert.assertTrue(KM1.matches(KM3));

    Assert.assertTrue(KM2.matches(KM0));
    Assert.assertFalse(KM2.matches(KM1));
    Assert.assertTrue(KM2.matches(KM2));
    Assert.assertTrue(KM2.matches(KM3));

    Assert.assertTrue(KM3.matches(KM0));
    Assert.assertTrue(KM3.matches(KM1));
    Assert.assertTrue(KM3.matches(KM2));
    Assert.assertTrue(KM3.matches(KM3));
  }

  @Test
  public void testIsValidStart()
  {
    Assert.assertTrue(KM0.isValidStart());
    Assert.assertTrue(KM1.isValidStart());
    Assert.assertTrue(KM2.isValidStart());
    Assert.assertTrue(KM3.isValidStart());
  }

  @Test
  public void testPlus()
  {
    Assert.assertEquals(
        KM("foo", ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        KM1.plus(KM3)
    );

    Assert.assertEquals(
        KM("foo", ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        KM0.plus(KM2)
    );

    Assert.assertEquals(
        KM("foo", ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        KM1.plus(KM2)
    );

    Assert.assertEquals(
        KM("foo", ImmutableMap.of(0, 2L, 1, 3L, 2, 5L)),
        KM2.plus(KM1)
    );

    Assert.assertEquals(
        KM("foo", ImmutableMap.of(0, 2L, 1, 4L, 2, 5L)),
        KM2.plus(KM2)
    );
  }

  private static KafkaDataSourceMetadata KM(String topic, Map<Integer, Long> offsets)
  {
    return new KafkaDataSourceMetadata(new KafkaPartitions(topic, offsets));
  }
}
