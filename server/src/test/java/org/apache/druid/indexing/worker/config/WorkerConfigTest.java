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

package org.apache.druid.indexing.worker.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class WorkerConfigTest
{
  @Test
  public void testSetters()
  {
    WorkerConfig config = new WorkerConfig()
        .cloneBuilder()
        .setCapacity(10)
        .setBaseTaskDirSize(100_000_000L)
        .setBaseTaskDirs(Arrays.asList("1", "2", "another"))
        .build();

    Assert.assertEquals(10, config.getCapacity());
    Assert.assertEquals(100_000_000L, config.getBaseTaskDirSize());
    Assert.assertEquals(Arrays.asList("1", "2", "another"), config.getBaseTaskDirs());
  }
}
