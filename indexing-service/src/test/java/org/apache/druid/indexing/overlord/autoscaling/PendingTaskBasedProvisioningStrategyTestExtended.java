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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.RemoteTaskRunner;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.CategorizedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerCategorySpec;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;


public class PendingTaskBasedProvisioningStrategyTestExtended
{
  private static final String DEFAULT_CATEGORY_1 = "default_category1";
  private static final String DEFAULT_CATEGORY_2 = "default_category2";
  private static final String CATEGORY_1 = "category1";
  private static final String CATEGORY_2 = "category2";
  private static final String TASK_TYPE_1 = "taskType1";
  private static final String TASK_TYPE_2 = "taskType2";
  private static final String TASK_TYPE_3 = "taskType3";
  private static final String DATA_SOURCE_1 = "ds1";
  private static final String DATA_SOURCE_2 = "ds2";
  private AutoScaler autoScalerDefault;
  private AutoScaler autoScalerCategory1;
  private AutoScaler autoScalerCategory2;
  private final List<AutoScaler> autoScalers = new ArrayList<>();
  private Task testTask;
  private PendingTaskBasedWorkerProvisioningConfig config;
  private final ScheduledExecutorService executorService = Execs.scheduledSingleThreaded("test service");
  private static final String MIN_VERSION = "2014-01-00T00:01:00Z";

  @Before
  public void setup()
  {
    autoScalerDefault = EasyMock.createMock(AutoScaler.class);

    autoScalerCategory1 = EasyMock.createMock(AutoScaler.class);
    autoScalerCategory2 = EasyMock.createMock(AutoScaler.class);

    autoScalers.clear();

    testTask = TestTasks.immediateSuccess("task1");

    config = new PendingTaskBasedWorkerProvisioningConfig()
        .setMaxScalingDuration(new Period(1000))
        .setNumEventsToTrack(10)
        .setPendingTaskTimeout(new Period(0))
        .setWorkerVersion(MIN_VERSION)
        .setMaxScalingStep(2);
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void testDefaultAutoscalerDidntSpawnInitialMinWorkers()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    // Strong affinity autoscaling mode will not use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 2, 4, Collections.emptyList(), 2, 1, 1, 1);
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 4, 6, Collections.emptyList(), 2, 1, 1, 1);
    autoScalers.addAll(Arrays.asList(autoScalerCategory1, autoScalerCategory2));

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    // No workers
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());
    // Expect this call two times because the both categorizied autoscalers will call it
    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(2);

    // Expect to create 2 workers
    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    ).times(2);

    // Expect to create 4 workers
    EasyMock.expect(autoScalerCategory2.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category2Node"))
    ).times(4);

    EasyMock.replay(runner, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);

    // In total expect provisioning of 2 + 4 = 6 workers
    Assert.assertEquals(6, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }

    EasyMock.verify(runner, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testDefaultAutoscalerSuccessfulMinWorkers()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    // Not strong affinity autoscaling mode will use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(
        autoScalerDefault,
        CategorizedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY,
        3,
        5,
        Collections.emptyList()
    );
    autoScalers.add(autoScalerDefault);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());
    // 1 node already running, only provision 2 more.
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    // Expect to create 2 workers
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(2);

    EasyMock.replay(runner, autoScalerDefault);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // Two workers should be provisioned
    Assert.assertEquals(2, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }

    EasyMock.verify(runner, autoScalerDefault);
  }

  @Test
  public void testAnyAutoscalerDontSpawnMinWorkers()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    // Strong affinity autoscaling mode will not use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // No pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(Collections.emptyList());

    // 1 worker already running. That means no initialization is required.
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable()
        )
    );

    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().isEmpty());

    EasyMock.verify(runner);
  }

  @Test
  public void testCategorizedAutoscalerSpawnedMinWorkers()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(
        false,
        TASK_TYPE_1,
        CATEGORY_1,
        DATA_SOURCE_1,
        CATEGORY_2
    );
    // Strong affinity autoscaling mode will not use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 5, 7, Collections.emptyList());
    autoScalers.add(autoScalerCategory1);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // One pending task
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Collections.singletonList(StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)));
    // No workers
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    // Expect to create 5 workers
    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    ).times(5);

    EasyMock.replay(runner, autoScalerCategory1);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // Five workers should be created
    Assert.assertEquals(5, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }

    EasyMock.verify(runner, autoScalerCategory1);
  }

  @Test
  public void testCategorizedAutoscalerSpawnedAdditionalWorker()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(
        false,
        TASK_TYPE_1,
        CATEGORY_1,
        DATA_SOURCE_1,
        CATEGORY_2
    );
    // Strong affinity autoscaling mode will not use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 2, 3, Collections.emptyList());
    autoScalers.add(autoScalerCategory1);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // One pending task
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Collections.singletonList(StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)));
    // Min workers are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    EasyMock.replay(runner, autoScalerCategory1);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // Expecting provisioning of one node for the pending task
    Assert.assertEquals(1, provisioner.getStats().toList().size());
    Assert.assertSame(provisioner.getStats().toList().get(0).getEvent(), ScalingStats.EVENT.PROVISION);

    EasyMock.verify(runner, autoScalerCategory1);
  }

  @Test
  public void testCategorizedAutoscalerSpawnedUpToMaxWorkers()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(
        false,
        TASK_TYPE_1,
        CATEGORY_1,
        DATA_SOURCE_1,
        CATEGORY_2
    );
    // Strong affinity autoscaling mode will not use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 2, 3, Collections.emptyList());
    autoScalers.add(autoScalerCategory1);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // Two pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Arrays.asList(
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)
            ));
    // Min workers are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    EasyMock.replay(runner, autoScalerCategory1);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // Can only spawn one worker because of maximum limit
    Assert.assertEquals(1, provisioner.getStats().toList().size());
    Assert.assertSame(provisioner.getStats().toList().get(0).getEvent(), ScalingStats.EVENT.PROVISION);

    EasyMock.verify(runner, autoScalerCategory1);
  }

  @Test
  public void testAllCategorizedAutoscalersStrongly()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(
        false,
        TASK_TYPE_1,
        new WorkerCategorySpec.CategoryConfig(
            DEFAULT_CATEGORY_1,
            ImmutableMap.of(
                DATA_SOURCE_1,
                CATEGORY_1,
                DATA_SOURCE_2,
                CATEGORY_2
            )
        ),
        TASK_TYPE_2,
        new WorkerCategorySpec.CategoryConfig(
            DEFAULT_CATEGORY_2,
            ImmutableMap.of(
                DATA_SOURCE_1,
                CATEGORY_1,
                DATA_SOURCE_2,
                CATEGORY_2
            )
        )
    );
    // Strong affinity autoscaling mode will not use default autoscaler
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 1, 3, Collections.emptyList());
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 1, 3, Collections.emptyList());
    autoScalers.addAll(Arrays.asList(autoScalerCategory1, autoScalerCategory2));

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // Four pending tasks: three have their categorized autoscalers and one for default autoscaler
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Arrays.asList(
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_1),
                StrategyTestUtils.TestTask.create(TASK_TYPE_2, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_3, DATA_SOURCE_2)
            ));
    // Min workers number of the each category are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(2);

    // Expect to create 1 worker
    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    // Expect to create 2 workers
    EasyMock.expect(autoScalerCategory2.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category2Node"))
    ).times(2);

    EasyMock.replay(runner, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // In total expect provisioning of 1 + 2 = 3 workers
    Assert.assertEquals(3, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }

    EasyMock.verify(runner, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testAllCategorizedAutoscalersNotStrongMode()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(
        false,
        TASK_TYPE_1,
        new WorkerCategorySpec.CategoryConfig(
            DEFAULT_CATEGORY_1,
            ImmutableMap.of(
                DATA_SOURCE_1,
                CATEGORY_1,
                DATA_SOURCE_2,
                CATEGORY_2
            )
        ),
        TASK_TYPE_2,
        new WorkerCategorySpec.CategoryConfig(
            DEFAULT_CATEGORY_2,
            ImmutableMap.of(
                DATA_SOURCE_1,
                CATEGORY_1,
                DATA_SOURCE_2,
                CATEGORY_2
            )
        )
    );
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);
    StrategyTestUtils.setupAutoscaler(
        autoScalerDefault,
        CategorizedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY,
        3,
        5,
        Collections.emptyList()
    );
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 1, 3, Collections.emptyList());
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 1, 3, Collections.emptyList());
    autoScalers.addAll(Arrays.asList(autoScalerDefault, autoScalerCategory1, autoScalerCategory2));

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // Four pending tasks: three have their categorized autoscalers and one for default autoscaler
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Arrays.asList(
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_1),
                StrategyTestUtils.TestTask.create(TASK_TYPE_2, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_3, DATA_SOURCE_2)
            ));
    // Min workers of two categoriez are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(3);

    // Expect to create 3 workers
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(3);

    // Expect to create 1 worker
    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    // Expect to create 2 workers
    EasyMock.expect(autoScalerCategory2.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category2Node"))
    ).times(2);

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // In total expect provisioning of 3 + 1 + 2 = 6 workers
    Assert.assertEquals(6, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testAllCategorizedAutoscalersAlert() throws InterruptedException
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(3);
    EasyMock.replay(emitter);

    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(
        false,
        TASK_TYPE_1,
        new WorkerCategorySpec.CategoryConfig(
            DEFAULT_CATEGORY_1,
            ImmutableMap.of(
                DATA_SOURCE_1,
                CATEGORY_1,
                DATA_SOURCE_2,
                CATEGORY_2
            )
        ),
        TASK_TYPE_2,
        new WorkerCategorySpec.CategoryConfig(
            DEFAULT_CATEGORY_2,
            ImmutableMap.of(
                DATA_SOURCE_1,
                CATEGORY_1,
                DATA_SOURCE_2,
                CATEGORY_2
            )
        )
    );
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(3);
    EasyMock.expect(autoScalerDefault.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScalerDefault.getCategory())
            .andReturn(null)
            .times(4);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.emptyList()).times(2);
    EasyMock.expect(autoScalerDefault.terminateWithIds(EasyMock.anyObject()))
            .andReturn(null);

    EasyMock.expect(autoScalerCategory1.getMinNumWorkers()).andReturn(1);
    EasyMock.expect(autoScalerCategory1.getMaxNumWorkers()).andReturn(3);
    EasyMock.expect(autoScalerCategory1.getCategory()).andReturn(CATEGORY_1).times(8);
    EasyMock.expect(autoScalerCategory1.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.emptyList()).times(2);
    EasyMock.expect(autoScalerCategory1.terminateWithIds(EasyMock.anyObject()))
            .andReturn(null);

    EasyMock.expect(autoScalerCategory2.getMinNumWorkers()).andReturn(1);
    EasyMock.expect(autoScalerCategory2.getMaxNumWorkers()).andReturn(3);
    EasyMock.expect(autoScalerCategory2.getCategory()).andReturn(CATEGORY_2).times(8);
    EasyMock.expect(autoScalerCategory2.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.emptyList()).times(2);
    EasyMock.expect(autoScalerCategory2.terminateWithIds(EasyMock.anyObject()))
            .andReturn(null);

    autoScalers.addAll(Arrays.asList(autoScalerDefault, autoScalerCategory1, autoScalerCategory2));

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // Four pending tasks: three have their categorized autoscalers and one for default autoscaler
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Arrays.asList(
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_1),
                StrategyTestUtils.TestTask.create(TASK_TYPE_2, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_3, DATA_SOURCE_2)
            )).times(2);
    // Min workers of two categoriez are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    ).times(2);

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(3);

    // Expect to create 3 workers
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(3);

    // Expect to create 1 worker
    EasyMock.expect(autoScalerCategory1.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category1Node"))
    );

    // Expect to create 2 workers
    EasyMock.expect(autoScalerCategory2.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("category2Node"))
    ).times(2);

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(6, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
    DateTime createdTime = provisioner.getStats().toList().get(0).getTimestamp();

    Thread.sleep(2000);

    provisionedSomething = provisioner.doProvision();

    Assert.assertFalse(provisionedSomething);
    Assert.assertEquals(6, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }
    DateTime anotherCreatedTime = provisioner.getStats().toList().get(0).getTimestamp();
    Assert.assertEquals(createdTime, anotherCreatedTime);

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2, emitter);
  }

  @Test
  public void testNullWorkerConfig()
  {
    AtomicReference<WorkerBehaviorConfig> workerConfig = new AtomicReference<>(null);

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // One pending task
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Collections.singletonList(StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)));
    // Min workers of two categoriez are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    // No workers should be created
    Assert.assertTrue(provisioner.getStats().toList().isEmpty());

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testNullWorkerCategorySpecNotStrong()
  {
    WorkerCategorySpec workerCategorySpec = null;
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    StrategyTestUtils.setupAutoscaler(
        autoScalerDefault,
        CategorizedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY,
        1,
        3,
        Collections.emptyList()
    );
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 1, 2, Collections.emptyList());
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 1, 4, Collections.emptyList());
    autoScalers.addAll(Arrays.asList(autoScalerDefault, autoScalerCategory1, autoScalerCategory2));

    // Expect to create 2 workers for 3 tasks because of maxLimit
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    ).times(2);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    // Three pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Arrays.asList(
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_1),
                StrategyTestUtils.TestTask.create(TASK_TYPE_2, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)
            ));
    // Min workers of two categories and one default are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(3);

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    // Two workers for default autoscaler should be created
    Assert.assertEquals(2, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.PROVISION);
    }

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testNullWorkerCategorySpecStrong()
  {
    WorkerCategorySpec workerCategorySpec = null;
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );

    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 1, 2, Collections.emptyList());
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 1, 4, Collections.emptyList());
    autoScalers.addAll(Arrays.asList(autoScalerCategory1, autoScalerCategory2));

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);

    // Three pending tasks
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Arrays.asList(
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_1),
                StrategyTestUtils.TestTask.create(TASK_TYPE_2, DATA_SOURCE_2),
                StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)
            ));

    // Min workers of two categories and one default are running
    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(2);

    EasyMock.replay(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    // No workers should be created because of strong affinity
    Assert.assertTrue(provisioner.getStats().toList().isEmpty());

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testDoSuccessfulTerminateForAllCategories()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );
    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    StrategyTestUtils.setupAutoscaler(
        autoScalerDefault,
        CategorizedWorkerBehaviorConfig.DEFAULT_AUTOSCALER_CATEGORY,
        1,
        Collections.emptyList()
    );
    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 1, Collections.emptyList());
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 1, Collections.emptyList());
    autoScalers.addAll(Arrays.asList(autoScalerDefault, autoScalerCategory1, autoScalerCategory2));

    EasyMock.expect(autoScalerDefault.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.emptyList())
    );
    EasyMock.expect(autoScalerCategory1.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.emptyList())
    );
    EasyMock.expect(autoScalerCategory2.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.emptyList())
    );
    EasyMock.replay(autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);

    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(
                Arrays.asList(
                    new StrategyTestUtils.TestZkWorker(testTask).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).getWorker()
                )
            ).times(3);

    EasyMock.expect(runner.getLazyWorkers()).andReturn(Collections.emptyList()).times(3);
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    Assert.assertTrue(terminatedSomething);
    Assert.assertEquals(3, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.TERMINATE);
    }

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testDoSuccessfulTerminateWithoutDefaultAutoscaler()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );
    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    StrategyTestUtils.setupAutoscaler(autoScalerCategory1, CATEGORY_1, 1, Collections.emptyList());
    StrategyTestUtils.setupAutoscaler(autoScalerCategory2, CATEGORY_2, 1, Collections.emptyList());
    autoScalers.addAll(Arrays.asList(autoScalerCategory1, autoScalerCategory2));

    EasyMock.expect(autoScalerCategory1.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.emptyList())
    );
    EasyMock.expect(autoScalerCategory2.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.emptyList())
    );
    EasyMock.replay(autoScalerCategory1, autoScalerCategory2);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);

    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    );

    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(
                Arrays.asList(
                    new StrategyTestUtils.TestZkWorker(testTask).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).getWorker()
                )
            ).times(2);

    EasyMock.expect(runner.getLazyWorkers()).andReturn(Collections.emptyList()).times(2);
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    Assert.assertTrue(terminatedSomething);
    Assert.assertEquals(2, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.TERMINATE);
    }

    EasyMock.verify(runner, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testSomethingTerminating()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );
    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(1);
    EasyMock.expect(autoScalerDefault.getCategory())
            .andReturn(null)
            .times(4);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip")).times(2);
    EasyMock.expect(autoScalerDefault.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.singletonList("ip"))
    );

    EasyMock.expect(autoScalerCategory1.getMinNumWorkers()).andReturn(1);
    EasyMock.expect(autoScalerCategory1.getCategory()).andReturn(CATEGORY_1).times(8);
    EasyMock.expect(autoScalerCategory1.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip")).times(2);
    EasyMock.expect(autoScalerCategory1.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.singletonList("ip"))
    );

    EasyMock.expect(autoScalerCategory2.getMinNumWorkers()).andReturn(1);
    EasyMock.expect(autoScalerCategory2.getCategory()).andReturn(CATEGORY_2).times(8);
    EasyMock.expect(autoScalerCategory2.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip")).times(2);
    EasyMock.expect(autoScalerCategory2.terminate(EasyMock.anyObject())).andReturn(
        new AutoScalingData(Collections.singletonList("ip"))
    );

    autoScalers.addAll(Arrays.asList(autoScalerDefault, autoScalerCategory1, autoScalerCategory2));

    EasyMock.replay(autoScalerDefault, autoScalerCategory1, autoScalerCategory2);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);

    EasyMock.expect(runner.getWorkers()).andReturn(
        Arrays.asList(
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable(),
            new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).toImmutable()
        )
    ).times(2);

    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(
                Arrays.asList(
                    new StrategyTestUtils.TestZkWorker(testTask).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_1).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).getWorker(),
                    new StrategyTestUtils.TestZkWorker(testTask, CATEGORY_2).getWorker()
                )
            ).times(3);

    EasyMock.expect(runner.getLazyWorkers()).andReturn(Collections.emptyList()).times(6);
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();

    Assert.assertTrue(terminatedSomething);
    Assert.assertEquals(3, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.TERMINATE);
    }

    terminatedSomething = provisioner.doTerminate();

    Assert.assertFalse(terminatedSomething);
    Assert.assertEquals(3, provisioner.getStats().toList().size());
    for (ScalingStats.ScalingEvent event : provisioner.getStats().toList()) {
      Assert.assertSame(event.getEvent(), ScalingStats.EVENT.TERMINATE);
    }

    EasyMock.verify(runner, autoScalerDefault, autoScalerCategory1, autoScalerCategory2);
  }

  @Test
  public void testMinCountIncrease()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );
    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    // Don't terminate anything
    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.expect(autoScalerDefault.getCategory()).andReturn(null).times(2);
    EasyMock.replay(autoScalerDefault);
    autoScalers.add(autoScalerDefault);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTaskPayloads()).andReturn(
        Collections.emptyList()
    ).times(2);
    EasyMock.expect(runner.getWorkers()).andReturn(
        Collections.singletonList(
            new StrategyTestUtils.TestZkWorker(
                NoopTask.create(),
                "http",
                "h1",
                "i1",
                MIN_VERSION,
                1,
                DEFAULT_CATEGORY_1
            ).toImmutable()
        )
    ).times(3);
    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig()).times(2);

    EasyMock.expect(runner.getLazyWorkers()).andReturn(new ArrayList<>());
    EasyMock.expect(runner.markWorkersLazy(EasyMock.anyObject(), EasyMock.anyInt()))
            .andReturn(Collections.emptyList());
    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean terminatedSomething = provisioner.doTerminate();
    Assert.assertFalse(terminatedSomething);
    EasyMock.verify(autoScalerDefault);

    // Don't provision anything
    EasyMock.reset(autoScalerDefault);
    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScalerDefault.getMaxNumWorkers()).andReturn(2);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.expect(autoScalerDefault.getCategory()).andReturn(null).times(2);
    EasyMock.replay(autoScalerDefault);

    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertFalse(provisionedSomething);
    Assert.assertTrue(provisioner.getStats().toList().isEmpty());
    EasyMock.verify(autoScalerDefault);

    EasyMock.reset(autoScalerDefault);
    // Increase minNumWorkers and expect provisioning
    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(3);
    EasyMock.expect(autoScalerDefault.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    // Should provision two new workers
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("h3"))
    ).times(3);
    EasyMock.expect(autoScalerDefault.getCategory()).andReturn(null).times(2);
    EasyMock.replay(autoScalerDefault);
    provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    EasyMock.verify(autoScalerDefault);
    EasyMock.verify(runner);
  }

  @Test
  public void testMinCountIncreaseNoWorkers()
  {
    WorkerCategorySpec workerCategorySpec = StrategyTestUtils.createWorkerCategorySpec(false);
    AtomicReference<WorkerBehaviorConfig> workerConfig = StrategyTestUtils.createWorkerConfigRef(
        workerCategorySpec,
        autoScalers
    );
    PendingTaskBasedWorkerProvisioningStrategy strategy = createStrategy(workerConfig);

    // Expect min number to be zero, but autoscaling should work for that case as well even there is no workers running
    EasyMock.expect(autoScalerDefault.getMinNumWorkers()).andReturn(0);
    EasyMock.expect(autoScalerDefault.getMaxNumWorkers()).andReturn(5);
    EasyMock.expect(autoScalerDefault.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(Collections.singletonList("ip"));
    EasyMock.expect(autoScalerDefault.provision()).andReturn(
        new AutoScalingData(Collections.singletonList("aNode"))
    );
    EasyMock.expect(autoScalerDefault.getCategory()).andReturn(null).times(2);

    EasyMock.replay(autoScalerDefault);
    autoScalers.add(autoScalerDefault);

    RemoteTaskRunner runner = EasyMock.createMock(RemoteTaskRunner.class);
    EasyMock.expect(runner.getPendingTaskPayloads())
            .andReturn(Collections.singletonList(StrategyTestUtils.TestTask.create(TASK_TYPE_1, DATA_SOURCE_2)));
    EasyMock.expect(runner.getWorkers()).andReturn(Collections.emptyList());
    EasyMock.expect(runner.getConfig()).andReturn(new RemoteTaskRunnerConfig());

    EasyMock.replay(runner);

    Provisioner provisioner = strategy.makeProvisioner(runner);
    boolean provisionedSomething = provisioner.doProvision();
    Assert.assertTrue(provisionedSomething);
    Assert.assertEquals(1, provisioner.getStats().toList().size());
    Assert.assertSame(provisioner.getStats().toList().get(0).getEvent(), ScalingStats.EVENT.PROVISION);

    EasyMock.verify(autoScalerDefault, runner);
  }

  private PendingTaskBasedWorkerProvisioningStrategy createStrategy(
      AtomicReference<WorkerBehaviorConfig> workerConfigRef
  )
  {
    return new PendingTaskBasedWorkerProvisioningStrategy(
        config,
        DSuppliers.of(workerConfigRef),
        new ProvisioningSchedulerConfig(),
        () -> executorService
    );
  }
}
