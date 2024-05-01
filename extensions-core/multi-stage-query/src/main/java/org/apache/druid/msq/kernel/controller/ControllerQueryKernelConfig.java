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

package org.apache.druid.msq.kernel.controller;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.msq.indexing.destination.MSQDestination;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Configuration for {@link ControllerQueryKernel}.
 */
public class ControllerQueryKernelConfig
{
  private final int maxRetainedPartitionSketchBytes;
  private final int maxConcurrentStages;
  private final boolean pipeline;
  private final boolean durableStorage;
  private final boolean faultTolerance;
  private final MSQDestination destination;

  @Nullable
  private final String controllerId;

  @Nullable
  private final List<String> workerIds;

  private ControllerQueryKernelConfig(
      int maxRetainedPartitionSketchBytes,
      int maxConcurrentStages,
      boolean pipeline,
      boolean durableStorage,
      boolean faultTolerance,
      MSQDestination destination,
      @Nullable String controllerId,
      @Nullable List<String> workerIds
  )
  {
    if (maxRetainedPartitionSketchBytes <= 0) {
      throw new IAE("maxRetainedPartitionSketchBytes must be positive");
    }

    if (pipeline && maxConcurrentStages < 2) {
      throw new IAE("maxConcurrentStagesPerWorker must be >= 2 when pipelining");
    }

    if (maxConcurrentStages <= 0) {
      throw new IAE("maxConcurrentStagesPerWorker must be positive");
    }

    if (pipeline && faultTolerance) {
      throw new IAE("Cannot pipeline with fault tolerance");
    }

    if (pipeline && durableStorage) {
      throw new IAE("Cannot pipeline with durable storage");
    }

    if (faultTolerance && !durableStorage) {
      throw new IAE("Cannot have fault tolerance without durable storage");
    }

    this.maxRetainedPartitionSketchBytes = maxRetainedPartitionSketchBytes;
    this.maxConcurrentStages = maxConcurrentStages;
    this.pipeline = pipeline;
    this.durableStorage = durableStorage;
    this.faultTolerance = faultTolerance;
    this.destination = destination;
    this.controllerId = controllerId;
    this.workerIds = workerIds;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public int getMaxRetainedPartitionSketchBytes()
  {
    return maxRetainedPartitionSketchBytes;
  }

  public int getMaxConcurrentStages()
  {
    return maxConcurrentStages;
  }

  public boolean isPipeline()
  {
    return pipeline;
  }

  public boolean isDurableStorage()
  {
    return durableStorage;
  }

  public boolean isFaultTolerant()
  {
    return faultTolerance;
  }

  public MSQDestination getDestination()
  {
    return destination;
  }

  @Nullable
  public List<String> getWorkerIds()
  {
    return workerIds;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ControllerQueryKernelConfig that = (ControllerQueryKernelConfig) o;
    return maxRetainedPartitionSketchBytes == that.maxRetainedPartitionSketchBytes
           && maxConcurrentStages == that.maxConcurrentStages
           && pipeline == that.pipeline
           && durableStorage == that.durableStorage
           && faultTolerance == that.faultTolerance
           && Objects.equals(controllerId, that.controllerId)
           && Objects.equals(workerIds, that.workerIds);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        maxRetainedPartitionSketchBytes,
        maxConcurrentStages,
        pipeline,
        durableStorage,
        faultTolerance,
        controllerId,
        workerIds
    );
  }

  @Override
  public String toString()
  {
    return "ControllerQueryKernelConfig{" +
           "maxRetainedPartitionSketchBytes=" + maxRetainedPartitionSketchBytes +
           ", maxConcurrentStages=" + maxConcurrentStages +
           ", pipeline=" + pipeline +
           ", durableStorage=" + durableStorage +
           ", faultTolerant=" + faultTolerance +
           ", controllerId='" + controllerId + '\'' +
           ", workerIds=" + workerIds +
           '}';
  }

  public static class Builder
  {
    private int maxRetainedPartitionSketchBytes = -1;
    private int maxConcurrentStages = 1;
    private boolean pipeline;
    private boolean durableStorage;
    private boolean faultTolerant;
    private MSQDestination destination;
    private String controllerId;
    private List<String> workerIds;

    /**
     * Use {@link #builder()}.
     */
    private Builder()
    {
    }

    public Builder maxRetainedPartitionSketchBytes(final int maxRetainedPartitionSketchBytes)
    {
      this.maxRetainedPartitionSketchBytes = maxRetainedPartitionSketchBytes;
      return this;
    }

    public Builder maxConcurrentStages(final int maxConcurrentStages)
    {
      this.maxConcurrentStages = maxConcurrentStages;
      return this;
    }

    public Builder pipeline(final boolean pipeline)
    {
      this.pipeline = pipeline;
      return this;
    }

    public Builder durableStorage(final boolean durableStorage)
    {
      this.durableStorage = durableStorage;
      return this;
    }

    public Builder faultTolerance(final boolean faultTolerant)
    {
      this.faultTolerant = faultTolerant;
      return this;
    }

    public Builder destination(final MSQDestination destination)
    {
      this.destination = destination;
      return this;
    }

    public Builder controllerId(final String controllerId)
    {
      this.controllerId = controllerId;
      return this;
    }

    public Builder workerIds(final List<String> workerIds)
    {
      this.workerIds = workerIds;
      return this;
    }

    public ControllerQueryKernelConfig build()
    {
      return new ControllerQueryKernelConfig(
          maxRetainedPartitionSketchBytes,
          maxConcurrentStages,
          pipeline,
          durableStorage,
          faultTolerant,
          destination,
          controllerId,
          workerIds
      );
    }
  }
}
