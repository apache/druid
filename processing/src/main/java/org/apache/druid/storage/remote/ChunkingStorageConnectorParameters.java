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

package org.apache.druid.storage.remote;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;

import java.io.File;
import java.util.function.Supplier;

// Start inclusive, end exclusive
public class ChunkingStorageConnectorParameters<T>
{
  private final long start;
  private final long end;
  private final String cloudStoragePath;
  private final ChunkingStorageConnector.GetObjectFromRangeFunction<T> objectSupplier;
  private final ObjectOpenFunction<T> objectOpenFunction;
  private final Predicate<Throwable> retryCondition;
  private final int maxRetry;
  private final Supplier<File> tempDirSupplier;

  public ChunkingStorageConnectorParameters(
      long start,
      long end,
      String cloudStoragePath,
      ChunkingStorageConnector.GetObjectFromRangeFunction<T> objectSupplier,
      ObjectOpenFunction<T> objectOpenFunction,
      Predicate<Throwable> retryCondition,
      int maxRetry,
      Supplier<File> tempDirSupplier
  )
  {
    this.start = start;
    this.end = end;
    this.cloudStoragePath = cloudStoragePath;
    this.objectSupplier = objectSupplier;
    this.objectOpenFunction = objectOpenFunction;
    this.retryCondition = retryCondition;
    this.maxRetry = maxRetry;
    this.tempDirSupplier = tempDirSupplier;
  }

  public long getStart()
  {
    return start;
  }

  public long getEnd()
  {
    return end;
  }

  public String getCloudStoragePath()
  {
    return cloudStoragePath;
  }

  public ChunkingStorageConnector.GetObjectFromRangeFunction<T> getObjectSupplier()
  {
    return objectSupplier;
  }

  public ObjectOpenFunction<T> getObjectOpenFunction()
  {
    return objectOpenFunction;
  }

  public Predicate<Throwable> getRetryCondition()
  {
    return retryCondition;
  }

  public int getMaxRetry()
  {
    return maxRetry;
  }

  public Supplier<File> getTempDirSupplier()
  {
    return tempDirSupplier;
  }

  private static class Builder<T>
  {
    private long start;
    private long end;
    private String cloudStoragePath;
    private ChunkingStorageConnector.GetObjectFromRangeFunction<T> objectSupplier;
    private ObjectOpenFunction<T> objectOpenFunction;
    private Predicate<Throwable> retryCondition;
    private int maxRetry;
    private Supplier<File> tempDirSupplier;


    public Builder<T> start(long start)
    {
      this.start = start;
      return this;
    }

    public Builder<T> end(long end)
    {
      this.end = end;
      return this;
    }

    public Builder<T> cloudStoragePath(String cloudStoragePath)
    {
      this.cloudStoragePath = cloudStoragePath;
      return this;
    }

    public Builder<T> objectSupplier(ChunkingStorageConnector.GetObjectFromRangeFunction<T> objectSupplier)
    {
      this.objectSupplier = objectSupplier;
      return this;
    }

    public Builder<T> objectOpenFunction(ObjectOpenFunction<T> objectOpenFunction)
    {
      this.objectOpenFunction = objectOpenFunction;
      return this;
    }

    public Builder<T> retryCondition(Predicate<Throwable> retryCondition)
    {
      this.retryCondition = retryCondition;
      return this;
    }

    public Builder<T> maxRetry(int maxRetry)
    {
      this.maxRetry = maxRetry;
      return this;
    }

    public Builder<T> tempDirSupplier(Supplier<File> tempDirSupplier)
    {
      this.tempDirSupplier = tempDirSupplier;
      return this;
    }

    ChunkingStorageConnectorParameters<T> build()
    {
      Preconditions.checkArgument(start >= 0, "'start' not provided or an incorrect value [%s] passed", start);
      Preconditions.checkArgument(end >= 0, "'end' not provided or an incorrect value [%s] passed", end);
      Preconditions.checkArgument(maxRetry >= 0, "'maxRetry' not provided or an incorrect value [%s] passed", maxRetry);
      return new ChunkingStorageConnectorParameters(
          start,
          end,
          Preconditions.checkNotNull(cloudStoragePath, "'cloudStoragePath' not supplied"),
          Preconditions.checkNotNull(objectSupplier, "'objectSupplier' not supplied"),
          Preconditions.checkNotNull(objectOpenFunction, "'objectOpenFunction' not supplied"),
          Preconditions.checkNotNull(retryCondition, "'retryCondition' not supplied"),
          maxRetry,
          Preconditions.checkNotNull(tempDirSupplier, "'tempDirSupplier' not supplied")
      );
    }
  }
}
