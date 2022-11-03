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

package org.apache.druid.query.groupby;

import com.google.common.base.Preconditions;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.TestBufferPool;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.Closeable;

public class TestGroupByBuffers implements Closeable
{
  private final int bufferSize;
  private final int numMergeBuffers;

  @Nullable
  private TestBufferPool processingPool;

  @Nullable
  private TestBufferPool mergePool;

  public TestGroupByBuffers(final int bufferSize, final int numMergeBuffers)
  {
    this.bufferSize = bufferSize;
    this.numMergeBuffers = numMergeBuffers;
    this.processingPool = TestBufferPool.offHeap(bufferSize, Integer.MAX_VALUE);
    this.mergePool = TestBufferPool.offHeap(bufferSize, numMergeBuffers);
  }

  public static TestGroupByBuffers createFromProcessingConfig(final DruidProcessingConfig config)
  {
    return new TestGroupByBuffers(config.intermediateComputeSizeBytes(), config.getNumMergeBuffers());
  }

  public static TestGroupByBuffers createDefault()
  {
    return createFromProcessingConfig(GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG);
  }

  public int getBufferSize()
  {
    return bufferSize;
  }

  public int getNumMergeBuffers()
  {
    return numMergeBuffers;
  }

  public TestBufferPool getProcessingPool()
  {
    return Preconditions.checkNotNull(processingPool, "processingPool");
  }

  public TestBufferPool getMergePool()
  {
    return Preconditions.checkNotNull(mergePool, "mergePool");
  }

  @Override
  public void close()
  {
    if (processingPool != null) {
      Assert.assertEquals(0, processingPool.getOutstandingObjectCount());
      processingPool = null;
    }

    if (mergePool != null) {
      if (mergePool.getOutstandingObjectCount() != 0) {
        throw mergePool.getOutstandingExceptionsCreated().iterator().next();
      }
      mergePool = null;
    }
  }
}
