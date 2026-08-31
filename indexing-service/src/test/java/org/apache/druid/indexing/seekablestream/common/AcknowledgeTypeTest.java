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

package org.apache.druid.indexing.seekablestream.common;

import org.apache.druid.data.input.impl.ByteEntity;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class AcknowledgeTypeTest
{
  @Test
  public void testValuesContainsAllExpectedConstants()
  {
    final AcknowledgeType[] values = AcknowledgeType.values();
    Assert.assertEquals(4, values.length);
    Assert.assertEquals(AcknowledgeType.ACCEPT, values[0]);
    Assert.assertEquals(AcknowledgeType.RELEASE, values[1]);
    Assert.assertEquals(AcknowledgeType.REJECT, values[2]);
    Assert.assertEquals(AcknowledgeType.RENEW, values[3]);
  }

  @Test
  public void testValueOfRoundTripsForEachConstant()
  {
    for (AcknowledgeType type : AcknowledgeType.values()) {
      Assert.assertSame(type, AcknowledgeType.valueOf(type.name()));
    }
  }

  @Test
  public void testAcknowledgingRecordSupplierDefaultMethods()
  {
    final AcknowledgingRecordSupplier<String, Long, ByteEntity> stub =
        new AcknowledgingRecordSupplier<String, Long, ByteEntity>()
        {
          @Override
          public void subscribe(Set<String> topics)
          {
          }

          @Override
          public void unsubscribe()
          {
          }

          @Override
          public Set<String> subscription()
          {
            return Set.of();
          }

          @Override
          public List<OrderedPartitionableRecord<String, Long, ByteEntity>> poll(long timeoutMs)
          {
            return List.of();
          }

          @Override
          public void acknowledge(String partitionId, Long offset)
          {
          }

          @Override
          public void acknowledge(String partitionId, Long offset, AcknowledgeType type)
          {
          }

          @Override
          public void acknowledge(Map<String, Collection<Long>> offsets, AcknowledgeType type)
          {
          }

          @Override
          public Map<String, Optional<Exception>> commitSync()
          {
            return Map.of();
          }

          @Override
          public Set<String> getPartitionIds(String stream)
          {
            return Set.of();
          }

          @Override
          public void close()
          {
          }
        };

    stub.wakeup();
    Assert.assertFalse(stub.acquisitionLockTimeoutMs().isPresent());
  }
}
