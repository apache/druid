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

package org.apache.druid.k8s.overlord;

import com.google.common.base.Supplier;
import org.apache.druid.k8s.overlord.execution.DefaultKubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.KubernetesTaskRunnerDynamicConfig;
import org.apache.druid.k8s.overlord.execution.PodTemplateSelectStrategy;
import org.apache.druid.k8s.overlord.execution.TaskTypePodTemplateSelectStrategy;
import org.junit.Assert;
import org.junit.Test;

public class KubernetesTaskRunnerEffectiveConfigTest
{
  @Test
  public void test_getCapacity_usesStaticWhenDynamicNull()
  {
    KubernetesTaskRunnerStaticConfig staticConfig = KubernetesTaskRunnerConfig.builder()
        .withCapacity(7)
        .build();
    Supplier<KubernetesTaskRunnerDynamicConfig> dynamicSupplier = () -> null;
    KubernetesTaskRunnerEffectiveConfig effective = new KubernetesTaskRunnerEffectiveConfig(staticConfig, dynamicSupplier);

    Assert.assertEquals(7, effective.getCapacity().intValue());
  }

  @Test
  public void test_getCapacity_usesDynamicWhenProvided()
  {
    KubernetesTaskRunnerStaticConfig staticConfig = KubernetesTaskRunnerConfig.builder()
        .withCapacity(2)
        .build();
    Supplier<KubernetesTaskRunnerDynamicConfig> dynamicSupplier = () -> new DefaultKubernetesTaskRunnerDynamicConfig(null, 9);
    KubernetesTaskRunnerEffectiveConfig effective = new KubernetesTaskRunnerEffectiveConfig(staticConfig, dynamicSupplier);

    Assert.assertEquals(9, effective.getCapacity().intValue());
  }

  @Test
  public void test_getCapacity_usesStaticWhenDynamicNullCapacity()
  {
    KubernetesTaskRunnerStaticConfig staticConfig = KubernetesTaskRunnerConfig.builder()
        .withCapacity(7)
        .build();
    Supplier<KubernetesTaskRunnerDynamicConfig> dynamicSupplier = () -> new DefaultKubernetesTaskRunnerDynamicConfig(null, null);
    KubernetesTaskRunnerEffectiveConfig effective = new KubernetesTaskRunnerEffectiveConfig(staticConfig, dynamicSupplier);

    Assert.assertEquals(7, effective.getCapacity().intValue());
  }

  @Test
  public void test_getPodTemplateSelectStrategy_usesDefaultWhenDynamicNull()
  {
    KubernetesTaskRunnerStaticConfig staticConfig = KubernetesTaskRunnerConfig.builder().build();
    Supplier<KubernetesTaskRunnerDynamicConfig> dynamicSupplier = () -> null;
    KubernetesTaskRunnerEffectiveConfig effective = new KubernetesTaskRunnerEffectiveConfig(staticConfig, dynamicSupplier);

    PodTemplateSelectStrategy strategy = effective.getPodTemplateSelectStrategy();
    Assert.assertTrue(strategy instanceof TaskTypePodTemplateSelectStrategy);
    Assert.assertEquals(KubernetesTaskRunnerDynamicConfig.DEFAULT_STRATEGY, strategy);
  }

  @Test
  public void test_getPodTemplateSelectStrategy_usesDynamicWhenProvided()
  {
    KubernetesTaskRunnerStaticConfig staticConfig = KubernetesTaskRunnerConfig.builder().build();
    PodTemplateSelectStrategy custom = new TaskTypePodTemplateSelectStrategy();
    Supplier<KubernetesTaskRunnerDynamicConfig> dynamicSupplier = () -> new DefaultKubernetesTaskRunnerDynamicConfig(custom, null);
    KubernetesTaskRunnerEffectiveConfig effective = new KubernetesTaskRunnerEffectiveConfig(staticConfig, dynamicSupplier);

    Assert.assertEquals(custom, effective.getPodTemplateSelectStrategy());
  }
}