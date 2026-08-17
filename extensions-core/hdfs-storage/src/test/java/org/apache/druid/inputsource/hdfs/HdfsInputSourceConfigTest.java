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

package org.apache.druid.inputsource.hdfs;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HdfsInputSourceConfigTest
{
  @Test
  public void testNullAllowedProtocolsUseDefault()
  {
    HdfsInputSourceConfig config = new HdfsInputSourceConfig(null);
    Assertions.assertEquals(HdfsInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS, config.getAllowedProtocols());
  }

  @Test
  public void testEmptyAllowedProtocolsUseDefault()
  {
    HdfsInputSourceConfig config = new HdfsInputSourceConfig(ImmutableSet.of());
    Assertions.assertEquals(HdfsInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS, config.getAllowedProtocols());
  }

  @Test
  public void testCustomAllowedProtocols()
  {
    HdfsInputSourceConfig config = new HdfsInputSourceConfig(ImmutableSet.of("druid"));
    Assertions.assertEquals(ImmutableSet.of("druid"), config.getAllowedProtocols());
  }
}
