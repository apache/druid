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

package org.apache.druid.server.scheduling;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.server.QuerySchedulerConfig;
import org.apache.druid.server.ResourceScheduleStrategy;
import org.apache.druid.server.initialization.ServerConfig;

import java.nio.ByteBuffer;

public class ArbitraryResourceScheduleStrategy implements ResourceScheduleStrategy
{
  @Override
  public BlockingPool<ByteBuffer> getMergeBufferPool(
      ServerConfig serverConfig,
      QuerySchedulerConfig querySchedulerConfig,
      DruidProcessingConfig config
  )
  {
    return new DefaultBlockingPool<>(
        new OffheapBufferGenerator("result merging", config.intermediateComputeSizeBytes()),
        config.getNumMergeBuffers()
    );
  }
}
