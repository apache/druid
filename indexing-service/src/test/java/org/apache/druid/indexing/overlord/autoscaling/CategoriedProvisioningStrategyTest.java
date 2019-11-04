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

import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.RemoteTaskRunner;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.FillCapacityWithCategorySpecWorkerSelectStrategy;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class CategoriedProvisioningStrategyTest
{
  private AutoScaler autoScalerDefault;
  private AutoScaler autoScalerCategory1;
  private AutoScaler autoScalerCategory2;
  private final Map<String, AutoScaler> categoryAutoScaler = new HashMap<>();
  private Task testTask;
  private CategoriedProvisioningConfig config;
  private final ScheduledExecutorService executorService = Execs.scheduledSingleThreaded("test service");
  private static final String MIN_VERSION = "2014-01-00T00:01:00Z";
  private static final String INVALID_VERSION = "0";

  @Before
  public void setup()
  {
    autoScalerDefault = EasyMock.createMock(AutoScaler.class);

    autoScalerCategory1 = EasyMock.createMock(AutoScaler.class);
    autoScalerCategory2 = EasyMock.createMock(AutoScaler.class);

    categoryAutoScaler.clear();
    categoryAutoScaler.put("category1", autoScalerCategory1);
    categoryAutoScaler.put("category2", autoScalerCategory2);

    testTask = TestTasks.immediateSuccess("task1");

    config = new CategoriedProvisioningConfig()
        .setMaxScalingDuration(new Period(1000))
        .setNumEventsToTrack(10)
        .setPendingTaskTimeout(new Period(0))
        .setWorkerVersion(MIN_VERSION)
        .setMaxScalingStep(2);
  }

  @Test
  public void testDefaultAutoscalerSuccessfullInitialMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(false);

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig);
    setupAutoscaler(autoScalerDefault, 3, 5, Collections.emptyList());
    setupAutoscaler(autoScalerCategory1, 2, 4, Collections.emptyList());
    setupAutoscaler(autoScalerCategory2, 4, 6, Collections.emptyList());

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());
    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(3);

    EasyMock.replay(runner, autoScalerDefault);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(3, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
  }

  @Test
  public void testStrongAssigmentDoesntInitialMinWorkers()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = createWorkerConfigRef(false);

    CategoriedProvisioningStrategy strategy = createStrategy(workerConfig);

    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(3);
    EasyMock.expect(autoScalerDefault.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.emptyList());
    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(
        Collections.emptyList()
    );
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.emptyList()
    );
    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(3);
    EasyMock.replay(runner, autoScalerDefault);


    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().size() == 3);
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertTrue(
          event.getEvent() == ScalingStats.EVENT.PROVISION
      );
    }
  }


  private void setupAutoscaler(AutoScaler autoScaler, int minWorkers, int maxWorkers, List<String> pendingTasks) {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(minWorkers);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(maxWorkers);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(pendingTasks);
  }

  private AtomicReference<WorkerBehaviorConfig> createWorkerConfigRef(boolean isStrong)
  {
    return new AtomicReference<>(
        new CategoriedWorkerBehaviorConfig(
            new FillCapacityWithCategorySpecWorkerSelectStrategy(null),
            autoScalerDefault,
            categoryAutoScaler,
            isStrong
        )
    );
  }

  private CategoriedProvisioningStrategy createStrategy(AtomicReference<WorkerBehaviorConfig> workerConfigRef)
  {
    return new CategoriedProvisioningStrategy(
        config,
        DSuppliers.of(workerConfigRef),
        new ProvisioningSchedulerConfig(),
        () -> executorService
    );
  }
}
