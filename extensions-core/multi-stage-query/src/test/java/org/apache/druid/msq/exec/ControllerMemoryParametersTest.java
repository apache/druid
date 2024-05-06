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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.sql.calcite.util.TestLookupProvider;
import org.junit.Assert;
import org.junit.Test;

public class ControllerMemoryParametersTest
{
  private static final double USABLE_MEMORY_FRACTION = 0.8;
  private static final int NUM_PROCESSORS_IN_JVM = 2;

  @Test
  public void test_oneQueryInJvm()
  {
    final ControllerMemoryParameters memoryParameters = ControllerMemoryParameters.createProductionInstance(
        makeMemoryIntrospector(128_000_000, 1),
        1
    );

    Assert.assertEquals(100_400_000, memoryParameters.getPartitionStatisticsMaxRetainedBytes());
  }

  @Test
  public void test_oneQueryInJvm_oneHundredWorkers()
  {
    final ControllerMemoryParameters memoryParameters = ControllerMemoryParameters.createProductionInstance(
        makeMemoryIntrospector(256_000_000, 1),
        100
    );

    Assert.assertEquals(103_800_000, memoryParameters.getPartitionStatisticsMaxRetainedBytes());
  }

  @Test
  public void test_twoQueriesInJvm()
  {
    final ControllerMemoryParameters memoryParameters = ControllerMemoryParameters.createProductionInstance(
        makeMemoryIntrospector(128_000_000, 2),
        1
    );

    Assert.assertEquals(49_200_000, memoryParameters.getPartitionStatisticsMaxRetainedBytes());
  }

  @Test
  public void test_maxSized()
  {
    final ControllerMemoryParameters memoryParameters = ControllerMemoryParameters.createProductionInstance(
        makeMemoryIntrospector(1_000_000_000, 1),
        1
    );

    Assert.assertEquals(300_000_000, memoryParameters.getPartitionStatisticsMaxRetainedBytes());
  }

  @Test
  public void test_notEnoughMemory()
  {
    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> ControllerMemoryParameters.createProductionInstance(
            makeMemoryIntrospector(30_000_000, 1),
            1
        )
    );

    final NotEnoughMemoryFault fault = (NotEnoughMemoryFault) e.getFault();
    Assert.assertEquals(30_000_000, fault.getServerMemory());
    Assert.assertEquals(1, fault.getServerWorkers());
    Assert.assertEquals(NUM_PROCESSORS_IN_JVM, fault.getServerThreads());
    Assert.assertEquals(24_000_000, fault.getUsableMemory());
    Assert.assertEquals(33_750_000, fault.getSuggestedServerMemory());
  }

  @Test
  public void test_minimalMemory()
  {
    final ControllerMemoryParameters memoryParameters = ControllerMemoryParameters.createProductionInstance(
        makeMemoryIntrospector(33_750_000, 1),
        1
    );

    Assert.assertEquals(25_000_000, memoryParameters.getPartitionStatisticsMaxRetainedBytes());
  }

  private MemoryIntrospector makeMemoryIntrospector(
      final long totalMemoryInJvm,
      final int numQueriesInJvm
  )
  {
    return new MemoryIntrospectorImpl(
        new TestLookupProvider(ImmutableMap.of()),
        totalMemoryInJvm,
        USABLE_MEMORY_FRACTION,
        numQueriesInJvm,
        NUM_PROCESSORS_IN_JVM
    );
  }
}
