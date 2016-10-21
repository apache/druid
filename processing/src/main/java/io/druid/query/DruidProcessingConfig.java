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

package io.druid.query;

import io.druid.java.util.common.concurrent.ExecutorServiceConfig;
import io.druid.segment.column.ColumnConfig;
import org.skife.config.Config;

public abstract class DruidProcessingConfig extends ExecutorServiceConfig implements ColumnConfig
{
  @Config({"druid.computation.buffer.size", "${base_path}.buffer.sizeBytes"})
  public int intermediateComputeSizeBytes()
  {
    return 1024 * 1024 * 1024;
  }

  @Config({"druid.computation.buffer.poolCacheMaxCount", "${base_path}.buffer.poolCacheMaxCount"})
  public int poolCacheMaxCount()
  {
    return Integer.MAX_VALUE;
  }

  @Override
  @Config(value = "${base_path}.numThreads")
  public int getNumThreads()
  {
    // default to leaving one core for background tasks
    final int processors = Runtime.getRuntime().availableProcessors();
    return processors > 1 ? processors - 1 : processors;
  }

  @Config("${base_path}.numMergeBuffers")
  public int getNumMergeBuffers()
  {
    return 0;
  }

  @Config(value = "${base_path}.columnCache.sizeBytes")
  public int columnCacheSizeBytes()
  {
    return 0;
  }

  @Config(value = "${base_path}.fifo")
  public boolean isFifo()
  {
    return false;
  }
}
