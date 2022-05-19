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

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.java.util.common.concurrent.Execs;

/**
 * {@link QueryProcessingPool} wrapper over {@link Execs#directExecutor()}
 */
public class DirectQueryProcessingPool extends ForwardingListeningExecutorService implements QueryProcessingPool
{
  public static DirectQueryProcessingPool INSTANCE = new DirectQueryProcessingPool();

  private DirectQueryProcessingPool()
  {

  }

  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task)
  {
    return delegate().submit(task);
  }

  @Override
  public ListeningExecutorService delegate()
  {
    return Execs.directExecutor();
  }
}
