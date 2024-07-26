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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

public class DruidProcessingConfig implements ColumnConfig
{
  private static final Logger log = new Logger(DruidProcessingConfig.class);

  @JsonProperty
  private final String formatString;
  @JsonProperty
  private final int numThreads;
  @JsonProperty
  private final int numMergeBuffers;
  @JsonProperty
  private final boolean fifo;
  @JsonProperty
  private final String tmpDir;
  @JsonProperty
  private final DruidProcessingBufferConfig buffer;
  @JsonProperty
  private final DruidProcessingIndexesConfig indexes;
  private final AtomicReference<Integer> computedBufferSizeBytes = new AtomicReference<>();
  private final boolean numThreadsConfigured;
  private final boolean numMergeBuffersConfigured;

  @JsonCreator
  public DruidProcessingConfig(
      @JsonProperty("formatString") @Nullable String formatString,
      @JsonProperty("numThreads") @Nullable Integer numThreads,
      @JsonProperty("numMergeBuffers") @Nullable Integer numMergeBuffers,
      @JsonProperty("fifo") @Nullable Boolean fifo,
      @JsonProperty("tmpDir") @Nullable String tmpDir,
      @JsonProperty("buffer") DruidProcessingBufferConfig buffer,
      @JsonProperty("indexes") DruidProcessingIndexesConfig indexes
  )
  {
    this.formatString = Configs.valueOrDefault(formatString, "processing-%s");
    this.numThreads = Configs.valueOrDefault(
        numThreads,
        Math.max(JvmUtils.getRuntimeInfo().getAvailableProcessors() - 1, 1)
    );
    this.numMergeBuffers = Configs.valueOrDefault(numMergeBuffers, Math.max(2, this.numThreads / 4));
    this.fifo = fifo == null || fifo;
    this.tmpDir = Configs.valueOrDefault(tmpDir, System.getProperty("java.io.tmpdir"));
    this.buffer = Configs.valueOrDefault(buffer, new DruidProcessingBufferConfig());
    this.indexes = Configs.valueOrDefault(indexes, new DruidProcessingIndexesConfig());

    this.numThreadsConfigured = numThreads != null;
    this.numMergeBuffersConfigured = numMergeBuffers != null;
    initializeBufferSize();
  }

  @VisibleForTesting
  public DruidProcessingConfig()
  {
    this(null, null, null, null, null, null, null);
  }

  private void initializeBufferSize()
  {
    HumanReadableBytes sizeBytesConfigured = this.buffer.getBufferSize();
    if (!DruidProcessingBufferConfig.DEFAULT_PROCESSING_BUFFER_SIZE_BYTES.equals(sizeBytesConfigured)) {
      if (sizeBytesConfigured.getBytes() > Integer.MAX_VALUE) {
        throw new IAE("druid.processing.buffer.sizeBytes must be less than 2GiB");
      }
      computedBufferSizeBytes.set(sizeBytesConfigured.getBytesInInt());
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
      directSizeBytes = Runtime.getRuntime().maxMemory() / 4;
      log.info("Using up to [%,d] bytes of direct memory for computation buffers.", directSizeBytes);
    }

    int totalNumBuffers = this.numMergeBuffers + this.numThreads;
    int sizePerBuffer = (int) ((double) directSizeBytes / (double) (totalNumBuffers + 1));

    final int computedSizePerBuffer = Math.min(
        sizePerBuffer,
        DruidProcessingBufferConfig.MAX_DEFAULT_PROCESSING_BUFFER_SIZE_BYTES
    );
    if (computedBufferSizeBytes.compareAndSet(null, computedSizePerBuffer)) {
      log.info(
          "Auto sizing buffers to [%,d] bytes each for [%,d] processing and [%,d] merge buffers. "
          + "If you run out of direct memory, you may need to set these parameters explicitly using the guidelines at "
          + "https://druid.apache.org/docs/latest/operations/basic-cluster-tuning.html#processing-threads-buffers.",
          computedSizePerBuffer,
          this.numThreads,
          this.numMergeBuffers
      );
    }
  }

  public String getFormatString()
  {
    return formatString;
  }

  public int getNumThreads()
  {
    return numThreads;
  }

  public int getNumMergeBuffers()
  {
    return numMergeBuffers;
  }

  public boolean isFifo()
  {
    return fifo;
  }

  public String getTmpDir()
  {
    return tmpDir;
  }

  public int intermediateComputeSizeBytes()
  {

    return computedBufferSizeBytes.get();
  }

  public int poolCacheMaxCount()
  {
    return buffer.getPoolCacheMaxCount();
  }

  public int getNumInitalBuffersForIntermediatePool()
  {
    return buffer.getPoolCacheInitialCount();
  }

  @Override
  public double skipValueRangeIndexScale()
  {
    return indexes.getSkipValueRangeIndexScale();
  }

  public boolean isNumThreadsConfigured()
  {
    return numThreadsConfigured;
  }

  public boolean isNumMergeBuffersConfigured()
  {
    return numMergeBuffersConfigured;
  }
}

