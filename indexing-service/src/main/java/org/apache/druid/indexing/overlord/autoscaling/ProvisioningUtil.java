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
import com.google.common.base.Supplier;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerSelectStrategy;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerCategorySpec;
import org.apache.druid.indexing.overlord.setup.WorkerSelectStrategy;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProvisioningUtil
{
  private static final EmittingLogger log = new EmittingLogger(ProvisioningUtil.class);

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

  @Nullable
  public static CategoriedWorkerBehaviorConfig getCategoriedWorkerBehaviorConfig(
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      String action
  )
  {
    final WorkerBehaviorConfig workerBehaviorConfig = workerConfigRef.get();
    if (workerBehaviorConfig == null) {
      log.error("No workerConfig available, cannot %s workers.", action);
      return null;
    }

    CategoriedWorkerBehaviorConfig workerConfig;
    if (workerBehaviorConfig instanceof DefaultWorkerBehaviorConfig) {
      AutoScaler autoscaler = ((DefaultWorkerBehaviorConfig) workerBehaviorConfig).getAutoScaler();
      WorkerSelectStrategy workerSelectStrategy = workerBehaviorConfig.getSelectStrategy();
      List<AutoScaler> autoscalers = autoscaler == null
                                     ? Collections.emptyList()
                                     : Collections.singletonList(autoscaler);
      workerConfig = new CategoriedWorkerBehaviorConfig(workerSelectStrategy, autoscalers);
    } else if (workerBehaviorConfig instanceof CategoriedWorkerBehaviorConfig) {
      workerConfig = (CategoriedWorkerBehaviorConfig) workerBehaviorConfig;
    } else {
      log.error(
          "Only DefaultWorkerBehaviorConfig or CategoriedWorkerBehaviorConfig are supported as WorkerBehaviorConfig, [%s] given, cannot %s workers",
          workerBehaviorConfig,
          action
      );
      return null;
    }
    if (workerConfig.getAutoScalers() == null || workerConfig.getAutoScalers().isEmpty()) {
      log.error("No autoScaler available, cannot %s workers", action);
      return null;
    }

    return workerConfig;
  }

  @Nullable
  public static WorkerCategorySpec getWorkerCategorySpec(CategoriedWorkerBehaviorConfig workerConfig)
  {
    if (workerConfig != null && workerConfig.getSelectStrategy() != null) {
      WorkerSelectStrategy selectStrategy = workerConfig.getSelectStrategy();
      if (selectStrategy instanceof CategoriedWorkerSelectStrategy) {
        return ((CategoriedWorkerSelectStrategy) selectStrategy).getWorkerCategorySpec();
      }
    }
    return null;
  }

  public static Map<String, AutoScaler> mapAutoscalerByCategory(List<AutoScaler> autoScalers)
  {
    Map<String, AutoScaler> result = autoScalers.stream().collect(Collectors.groupingBy(
        ProvisioningUtil::getAutoscalerCategory,
        Collectors.collectingAndThen(Collectors.toList(), values -> values.get(0))
    ));

    if (result.size() != autoScalers.size()) {
      log.warn(
          "Probably autoscalers with duplicated categories were defined. The first instance of each duplicate category will be used.");
    }

    return result;
  }

  @Nullable
  public static AutoScaler getAutoscalerByCategory(String category, Map<String, AutoScaler> autoscalersByCategory)
  {
    AutoScaler autoScaler = autoscalersByCategory.get(category);
    boolean isStrongAssignment = !autoscalersByCategory.containsKey(CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY);

    if (autoScaler == null && isStrongAssignment) {
      log.warn(
          "No autoscaler found for category %s. Tasks of this category will not be assigned to default autoscaler because of strong affinity.",
          category
      );
      return null;
    }
    return autoScaler == null
           ? autoscalersByCategory.get(CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY)
           : autoScaler;
  }

  public static Collection<Worker> getWorkersOfCategory(Collection<Worker> workers, String category) {
    return workers.stream().filter(worker -> category.equals(worker.getCategory())).collect(Collectors.toList());
  }

  public static String getAutoscalerCategory(AutoScaler autoScaler) {
    return autoScaler.getCategory() == null
           ? CategoriedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY
           : autoScaler.getCategory();
  }
}
