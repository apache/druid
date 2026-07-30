/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.druid.segment.data;

import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.SerializablePairLongStringComplexMetricSerde;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class ObjectStrategyBoundsTest
{
  @Test
  public void testRejectsInvalidSerializedLengths()
  {
    final List<ObjectStrategy<?>> strategies = List.of(
        GenericIndexed.UTF8_STRATEGY,
        new ImmutableRTreeObjectStrategy(new ConciseBitmapFactory()),
        new HyperUniquesSerde().getObjectStrategy(),
        new SerializablePairLongStringComplexMetricSerde().getObjectStrategy()
    );

    for (final ObjectStrategy<?> strategy : strategies) {
      final ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.position(1);

      Assert.assertThrows(IAE.class, () -> strategy.fromByteBuffer(buffer, -1));
      Assert.assertThrows(IAE.class, () -> strategy.fromByteBuffer(buffer, 4));
      Assert.assertEquals(1, buffer.position());
      Assert.assertEquals(4, buffer.limit());
    }
  }
}
