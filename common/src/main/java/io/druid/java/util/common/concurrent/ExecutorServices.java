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

package io.druid.java.util.common.concurrent;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.java.util.common.lifecycle.Lifecycle;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorServices
{
  public static ExecutorService create(Lifecycle lifecycle, ExecutorServiceConfig config)
  {
    return manageLifecycle(
        lifecycle,
        Executors.newFixedThreadPool(
            config.getNumThreads(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat(config.getFormatString()).build()
        )
    );
  }

  public static <T extends ExecutorService> T manageLifecycle(Lifecycle lifecycle, final T service)
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
            }

            @Override
            public void stop()
            {
              service.shutdownNow();
            }
          }
      );
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
    return service;
  }
}
