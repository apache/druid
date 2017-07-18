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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Predicate;

import io.druid.indexing.overlord.ImmutableWorkerInfo;
import io.druid.java.util.common.ISE;

public class ProvisioningUtil
{
  public static Predicate<ImmutableWorkerInfo> createValidWorkerPredicate(
      final SimpleWorkerProvisioningConfig config
  )
  {
    return new Predicate<ImmutableWorkerInfo>()
    {
      @Override
      public boolean apply(ImmutableWorkerInfo worker)
      {
        final String minVersion = config.getWorkerVersion();
        if (minVersion == null) {
          throw new ISE("No minVersion found! It should be set in your runtime properties or configuration database.");
        }
        return worker.isValidVersion(minVersion);
      }
    };
  }

  public static Predicate<ImmutableWorkerInfo> createLazyWorkerPredicate(
      final SimpleWorkerProvisioningConfig config
  )
  {
    final Predicate<ImmutableWorkerInfo> isValidWorker = createValidWorkerPredicate(config);

    return new Predicate<ImmutableWorkerInfo>()
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
