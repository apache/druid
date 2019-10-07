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

package org.apache.druid.query;

import org.apache.druid.java.util.common.concurrent.ExecutorServiceConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.utils.JvmUtils;
import org.skife.config.Config;

import java.util.concurrent.atomic.AtomicReference;

public abstract class DruidProcessingConfig extends ExecutorServiceConfig implements ColumnConfig
{
  private static final Logger log = new Logger(DruidProcessingConfig.class);

  public static final int DEFAULT_NUM_MERGE_BUFFERS = -1;
  public static final int DEFAULT_PROCESSING_BUFFER_SIZE_BYTES = -1;
  public static final int MAX_DEFAULT_PROCESSING_BUFFER_SIZE_BYTES = 1024 * 1024 * 1024;

  private AtomicReference<Integer> computedBufferSizeBytes = new AtomicReference<>();

  @Config({"druid.computation.buffer.size", "${base_path}.buffer.sizeBytes"})
  public int intermediateComputeSizeBytesConfigured()
  {
    return DEFAULT_PROCESSING_BUFFER_SIZE_BYTES;
  }

  public int intermediateComputeSizeBytes()
  {
    int sizeBytesConfigured = intermediateComputeSizeBytesConfigured();
    if (sizeBytesConfigured != DEFAULT_PROCESSING_BUFFER_SIZE_BYTES) {
      return sizeBytesConfigured;
    } else if (computedBufferSizeBytes.get() != null) {
      return computedBufferSizeBytes.get();
    }

    long directSizeBytes;
    try {
      directSizeBytes = JvmUtils.getRuntimeInfo().getDirectMemorySizeBytes();
      log.info(
          "Detected max direct memory size of [%,d] bytes",
          directSizeBytes
      );
    }
    catch (UnsupportedOperationException e) {
      // max direct memory defaults to max heap size on recent JDK version, unless set explicitly
      directSizeBytes = computeMaxMemoryFromMaxHeapSize();
      log.info(
          "Defaulting to at most [%,d] bytes (25%% of max heap size) of direct memory for computation buffers",
          directSizeBytes
      );
    }

    int numProcessingThreads = getNumThreads();
    int numMergeBuffers = getNumMergeBuffers();
    int totalNumBuffers = numMergeBuffers + numProcessingThreads;
    int sizePerBuffer = (int) ((double) directSizeBytes / (double) (totalNumBuffers + 1));

    final int computedSizePerBuffer = Math.min(sizePerBuffer, MAX_DEFAULT_PROCESSING_BUFFER_SIZE_BYTES);
    if (computedBufferSizeBytes.compareAndSet(null, computedSizePerBuffer)) {
      log.info(
          "Auto sizing buffers to [%,d] bytes each for [%,d] processing and [%,d] merge buffers",
          computedSizePerBuffer,
          numProcessingThreads,
          numMergeBuffers
      );
    }
    return computedSizePerBuffer;
  }

  public static long computeMaxMemoryFromMaxHeapSize()
  {
    return Runtime.getRuntime().maxMemory() / 4;
  }

  @Config({"druid.computation.buffer.poolCacheMaxCount", "${base_path}.buffer.poolCacheMaxCount"})
  public int poolCacheMaxCount()
  {
    return Integer.MAX_VALUE;
  }

  @Override
  @Config(value = "${base_path}.numThreads")
  public int getNumThreadsConfigured()
  {
    return DEFAULT_NUM_THREADS;
  }

  public int getNumMergeBuffers()
  {
    int numMergeBuffersConfigured = getNumMergeBuffersConfigured();
    if (numMergeBuffersConfigured != DEFAULT_NUM_MERGE_BUFFERS) {
      return numMergeBuffersConfigured;
    } else {
      return Math.max(2, getNumThreads() / 4);
    }
  }

  /**
   * Returns the number of merge buffers _explicitly_ configured, or -1 if it is not explicitly configured, that is not
   * a valid number of buffers. To get the configured value or the default (valid) number, use {@link
   * #getNumMergeBuffers()}. This method exists for ability to distinguish between the default value set when there is
   * no explicit config, and an explicitly configured value.
   */
  @Config("${base_path}.numMergeBuffers")
  public int getNumMergeBuffersConfigured()
  {
    return DEFAULT_NUM_MERGE_BUFFERS;
  }

  @Override
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

  @Config(value = "${base_path}.tmpDir")
  public String getTmpDir()
  {
    return System.getProperty("java.io.tmpdir");
  }
}
