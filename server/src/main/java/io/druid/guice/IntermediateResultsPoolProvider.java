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

package io.druid.guice;

import com.google.inject.ImplementedBy;
import io.druid.collections.StupidPool;
import io.druid.offheap.OffheapBufferGenerator;
import io.druid.query.DruidProcessingConfig;

import java.nio.ByteBuffer;

@ImplementedBy(HistoricalIntermediateResultsPoolProvider.class)
public abstract class IntermediateResultsPoolProvider
{
  private final int poolSize;
  private final DruidProcessingConfig processingConfig;

  IntermediateResultsPoolProvider(int poolSize, DruidProcessingConfig processingConfig)
  {
    this.poolSize = poolSize;
    this.processingConfig = processingConfig;
  }

  public StupidPool<ByteBuffer> getIntermediateResultsPool()
  {
    return new StupidPool<>(
        "intermediate processing pool",
        new OffheapBufferGenerator("intermediate processing", processingConfig.intermediateComputeSizeBytes()),
        poolSize,
        processingConfig.poolCacheMaxCount()
    );
  }

  public int getPoolSize()
  {
    return poolSize;
  }
}
