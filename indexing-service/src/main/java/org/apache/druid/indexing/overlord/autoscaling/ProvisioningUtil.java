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

package org.apache.druid.indexing.overlord.autoscaling;

import com.google.common.base.Predicate;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;

public class ProvisioningUtil
{
  public static Predicate<ImmutableWorkerInfo> createValidWorkerPredicate(
      final SimpleWorkerProvisioningConfig config,
      final WorkerTaskRunner runner
  )
  {
    return new Predicate<>()
    {
      @Override
      public boolean apply(ImmutableWorkerInfo worker)
      {
        return worker.isValidVersion(getMinWorkerVersion(config, runner));
      }
    };
  }

  /**
   * Returns {@code druid.indexer.autoscale.workerVersion} if it is set. Otherwise returns the runner's
   * {@code minWorkerVersion}, the version that tasks are assigned by, so that a worker which can no longer get
   * tasks does not count as valid.
   */
  private static String getMinWorkerVersion(
      final SimpleWorkerProvisioningConfig config,
      final WorkerTaskRunner runner
  )
  {
    final String workerVersion = config.getWorkerVersion();
    return workerVersion != null ? workerVersion : runner.getConfig().getMinWorkerVersion();
  }

  public static Predicate<ImmutableWorkerInfo> createLazyWorkerPredicate(
      final SimpleWorkerProvisioningConfig config,
      final WorkerTaskRunner runner
  )
  {
    final Predicate<ImmutableWorkerInfo> isValidWorker = createValidWorkerPredicate(config, runner);

    return new Predicate<>()
    {
      @Override
      public boolean apply(ImmutableWorkerInfo worker)
      {
        final boolean itHasBeenAWhile = System.currentTimeMillis() - worker.getLastCompletedTaskTime().getMillis()
                                        >= config.getWorkerIdleTimeout().toStandardDuration().getMillis();
        return itHasBeenAWhile || !isValidWorker.apply(worker);
      }
    };
  }

}
