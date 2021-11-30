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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.nio.ByteBuffer;

public class QueryBufferPoolStatsMonitor extends AbstractMonitor
{
  private final BlockingPool<ByteBuffer> mergeBufferPool;
  private final StupidPool<ByteBuffer> intermediateResultPool;

  @Inject
  public QueryBufferPoolStatsMonitor(
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      @Global NonBlockingPool<ByteBuffer> intermediateResultPool
  )
  {
    this.mergeBufferPool = mergeBufferPool;
    this.intermediateResultPool = (StupidPool<ByteBuffer>) intermediateResultPool;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    int mergeBufferMaxNum = mergeBufferPool.maxSize();
    emitter.emit(new ServiceMetricEvent.Builder().build("query/mergeBufferMaxNum", mergeBufferMaxNum));

    int mergeBufferAvailNum = ((DefaultBlockingPool<ByteBuffer>) mergeBufferPool).getPoolSize();
    emitter.emit(new ServiceMetricEvent.Builder().build("query/mergeBufferAvailNum", mergeBufferAvailNum));

    long intermediateResultPoolSize = intermediateResultPool.poolSize();
    emitter.emit(new ServiceMetricEvent.Builder().build("query/intermResultBufferAvailNum",
                                                        intermediateResultPoolSize));

    return true;
  }
}
