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

package io.druid.guice;

import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;

import javax.inject.Provider;
import java.util.concurrent.ForkJoinPool;

public class ForkJoinPoolProvider implements Provider<LifecycleForkJoinPool>
{
  private static final Logger LOG = new Logger(ForkJoinPoolProvider.class);

  private final String nameFormat;

  public ForkJoinPoolProvider(String nameFormat)
  {
    // Fail fast on bad name format
    StringUtils.format(nameFormat, 3);
    this.nameFormat = nameFormat;
  }

  @Override
  public LifecycleForkJoinPool get()
  {
    return new LifecycleForkJoinPool(
        // This should probably be configurable. Until then, just piggyback off the common pool's parallelism
        ForkJoinPool.commonPool().getParallelism(),
        pool -> Execs.makeWorkerThread(nameFormat, pool),
        (t, e) -> LOG.error(e, "Unhandled exception in thread [%s]", t),
        false
    );
  }
}
