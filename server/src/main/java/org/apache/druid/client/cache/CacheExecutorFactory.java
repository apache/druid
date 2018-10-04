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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public enum CacheExecutorFactory
{
  COMMON_FJP {
    @Override
    public Executor createExecutor()
    {
      return ForkJoinPool.commonPool();
    }
  },
  /** Could be configured in {@link CaffeineCacheConfig} */
  @SuppressWarnings("unused")
  SINGLE_THREAD {
    @Override
    public Executor createExecutor()
    {
      return Execs.singleThreaded("CaffeineWorker-%s");
    }
  },
  /** Could be configured in {@link CaffeineCacheConfig} */
  @SuppressWarnings("unused")
  SAME_THREAD {
    @Override
    public Executor createExecutor()
    {
      return Runnable::run;
    }
  };

  public abstract Executor createExecutor();

  @JsonCreator
  public static CacheExecutorFactory from(String str)
  {
    return Enum.valueOf(CacheExecutorFactory.class, StringUtils.toUpperCase(str));
  }
}
