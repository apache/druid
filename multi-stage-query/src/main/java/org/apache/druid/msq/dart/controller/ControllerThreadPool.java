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

package org.apache.druid.msq.dart.controller;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.Controller;

/**
 * Thread pool for running {@link Controller}. Number of threads is equal to
 * {@link DartControllerConfig#getConcurrentQueries()}, which limits the number of concurrent controllers.
 */
@ManageLifecycle
public class ControllerThreadPool
{
  private final ListeningExecutorService executorService;

  public ControllerThreadPool(final ListeningExecutorService executorService)
  {
    this.executorService = executorService;
  }

  public ListeningExecutorService getExecutorService()
  {
    return executorService;
  }

  @LifecycleStop
  public void stop()
  {
    executorService.shutdown();
  }
}
