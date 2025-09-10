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

package org.apache.druid.data.input;


import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

public class KafkaUtilsTest
{

  private static final byte[] BYTES = ByteBuffer.allocate(Integer.BYTES).putInt(42).array();

  @Test
  public void testNoopMethodHandle() throws Throwable
  {
    Assert.assertNull(
        KafkaUtils.noopMethodHandle().invoke(new ByteEntity(new byte[]{}))
    );
  }

  @Test
  public void testKafkaRecordEntity() throws Throwable
  {
    final MethodHandle handle = KafkaUtils.lookupGetHeaderMethod(KafkaUtilsTest.class.getClassLoader(), "version");
    KafkaRecordEntity input = new KafkaRecordEntity(
        new ConsumerRecord<>(
            "test",
            0,
            0,
            0,
            TimestampType.CREATE_TIME,
            -1L,
            -1,
            -1,
            null,
            new byte[]{},
            new RecordHeaders(ImmutableList.of(new Header()
            {
              @Override
              public String key()
              {
                return "version";
              }

              @Override
              public byte[] value()
              {
                return BYTES;
              }
            }))
        )
    );
    Assert.assertArrayEquals(BYTES, (byte[]) handle.invoke(input));
  }

  @Test(expected = ClassCastException.class)
  public void testNonKafkaEntity() throws Throwable
  {
    final MethodHandle handle = KafkaUtils.lookupGetHeaderMethod(KafkaUtilsTest.class.getClassLoader(), "version");
    handle.invoke(new ByteEntity(new byte[]{}));
  }
}
