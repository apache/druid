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

package org.apache.druid.segment.realtime.appenderator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MemoryParameterOverridingAppenderatorConfigTest
{
  @Mock
  private AppenderatorConfig baseConfig;

  @Test
  public void testGetMaxRowsInMemory_WhenBaseConfigReturnsZero()
  {
    when(baseConfig.getMaxRowsInMemory()).thenReturn(0);
    UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig config = 
        new UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig(
            baseConfig,
            1000L
        );

    assertEquals(Integer.MAX_VALUE, config.getMaxRowsInMemory());
  }

  @Test
  public void testGetMaxRowsInMemory_WhenBaseConfigReturnsNonZero()
  {
    when(baseConfig.getMaxRowsInMemory()).thenReturn(5000);
    UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig config = 
        new UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig(
            baseConfig,
            1000L
        );

    assertEquals(5000, config.getMaxRowsInMemory());
  }

  @Test
  public void testGetMaxBytesInMemory_WhenBaseConfigReturnsZero()
  {
    when(baseConfig.getMaxBytesInMemory()).thenReturn(0L);
    UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig config = 
        new UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig(
            baseConfig,
            1000L
        );

    assertEquals(1000L, config.getMaxBytesInMemory());
  }

  @Test
  public void testGetMaxBytesInMemory_WhenBaseConfigReturnsNonZero()
  {
    when(baseConfig.getMaxBytesInMemory()).thenReturn(2000L);
    UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig config = 
        new UnifiedIndexerAppenderatorsManager.MemoryParameterOverridingAppenderatorConfig(
            baseConfig,
            1000L
        );

    assertEquals(2000L, config.getMaxBytesInMemory());
  }
}
