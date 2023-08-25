package org.apache.druid.k8s.overlord;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.io.IOUtils;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ExecutionException;


@RunWith(EasyMockRunner.class)
public class KubernetesAndWorkerTaskRunnerTest extends EasyMockSupport {

  private static final String ID = "id";
  private static final String DATA_SOURCE = "dataSource";
  @Mock KubernetesTaskRunner kubernetesTaskRunner;
  @Mock HttpRemoteTaskRunner workerTaskRunner;

  KubernetesAndWorkerTaskRunner runner;

  private Task task;

  @Before
  public void setup()
  {
    task = NoopTask.create(DATA_SOURCE);
    runner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        new KubernetesAndWorkerTaskRunnerConfig(null, null)
    );
  }

  @Test
  public void test_runOnKubernetes() throws ExecutionException, InterruptedException {
    KubernetesAndWorkerTaskRunner kubernetesAndWorkerTaskRunner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        new KubernetesAndWorkerTaskRunnerConfig(null, false)
    );
    TaskStatus taskStatus = TaskStatus.success(ID);
    EasyMock.expect(kubernetesTaskRunner.run(task)).andReturn(Futures.immediateFuture(taskStatus));

    replayAll();
    Assert.assertEquals(taskStatus, kubernetesAndWorkerTaskRunner.run(task).get());
    verifyAll();
  }

  @Test
  public void test_runOnWorker() throws ExecutionException, InterruptedException {
    KubernetesAndWorkerTaskRunner kubernetesAndWorkerTaskRunner = new KubernetesAndWorkerTaskRunner(
        kubernetesTaskRunner,
        workerTaskRunner,
        new KubernetesAndWorkerTaskRunnerConfig(null, true)
    );
    TaskStatus taskStatus = TaskStatus.success(ID);
    EasyMock.expect(workerTaskRunner.run(task)).andReturn(Futures.immediateFuture(taskStatus));

    replayAll();
    Assert.assertEquals(taskStatus, kubernetesAndWorkerTaskRunner.run(task).get());
    verifyAll();
  }

  @Test
  public void test_getUsedCapacity() {
    EasyMock.expect(kubernetesTaskRunner.getUsedCapacity()).andReturn(1);
    EasyMock.expect(workerTaskRunner.getUsedCapacity()).andReturn(1);

    replayAll();
    Assert.assertEquals(2, runner.getUsedCapacity());
    verifyAll();
  }

  @Test
  public void test_getTotalCapacity() {
    EasyMock.expect(kubernetesTaskRunner.getTotalCapacity()).andReturn(1);
    EasyMock.expect(workerTaskRunner.getTotalCapacity()).andReturn(1);

    replayAll();
    Assert.assertEquals(2, runner.getTotalCapacity());
    verifyAll();
  }

  @Test
  public void test_subTaskRunners() {
    Assert.assertEquals(ImmutableList.of(kubernetesTaskRunner, workerTaskRunner), runner.getSubTaskRunners());
  }

  @Test
  public void test_getRunnerTaskState_kubernetes() {
    RunnerTaskState runnerTaskState = RunnerTaskState.RUNNING;
    EasyMock.expect(kubernetesTaskRunner.getRunnerTaskState(ID)).andReturn(runnerTaskState);

    replayAll();
    Assert.assertEquals(runnerTaskState, runner.getRunnerTaskState(ID));
    verifyAll();
  }

  @Test
  public void test_getRunnerTaskState_worker() {
    RunnerTaskState runnerTaskState = RunnerTaskState.RUNNING;
    EasyMock.expect(kubernetesTaskRunner.getRunnerTaskState(ID)).andReturn(null);
    EasyMock.expect(workerTaskRunner.getRunnerTaskState(ID)).andReturn(runnerTaskState);

    replayAll();
    Assert.assertEquals(runnerTaskState, runner.getRunnerTaskState(ID));
    verifyAll();
  }

  @Test
  public void test_streamTaskLog_kubernetes() throws IOException {
    InputStream inputStream = IOUtils.toInputStream("inputStream", Charset.defaultCharset());
    EasyMock.expect(kubernetesTaskRunner.streamTaskLog(ID, 0)).andReturn(Optional.of(inputStream));

    replayAll();
    Assert.assertEquals(inputStream, runner.streamTaskLog(ID, 0).get());
    verifyAll();
  }

  @Test
  public void test_streamTasklog_worker() throws IOException {
    InputStream inputStream = IOUtils.toInputStream("inputStream", Charset.defaultCharset());
    EasyMock.expect(kubernetesTaskRunner.streamTaskLog(ID, 0)).andReturn(Optional.absent());
    EasyMock.expect(workerTaskRunner.streamTaskLog(ID, 0)).andReturn(Optional.of(inputStream));

    replayAll();
    Assert.assertEquals(inputStream, runner.streamTaskLog(ID, 0).get());
    verifyAll();
  }

  @Test
  public void test_getBlacklistedTaskSlotCount() {
    Map<String, Long> blacklistedTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> blacklistedTaskSlots2 = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getBlacklistedTaskSlotCount()).andReturn(blacklistedTaskSlots);
    EasyMock.expect(workerTaskRunner.getBlacklistedTaskSlotCount()).andReturn(blacklistedTaskSlots2);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(blacklistedTaskSlots).putAll(blacklistedTaskSlots2).build(),
        runner.getBlacklistedTaskSlotCount()
    );
    verifyAll();
  }

  @Test
  public void test_getLazyTaskSlotCount() {
    Map<String, Long> lazyTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> lazyTaskSlots2 = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getLazyTaskSlotCount()).andReturn(lazyTaskSlots);
    EasyMock.expect(workerTaskRunner.getLazyTaskSlotCount()).andReturn(lazyTaskSlots2);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(lazyTaskSlots).putAll(lazyTaskSlots2).build(),
        runner.getLazyTaskSlotCount()
    );
    verifyAll();
  }

  @Test
  public void test_getIdleTaskSlotCount() {
    Map<String, Long> idleTaskSlots = ImmutableMap.of("category", 1L);
    Map<String, Long> idleTaskSlots2 = ImmutableMap.of("category2", 2L);

    EasyMock.expect(kubernetesTaskRunner.getLazyTaskSlotCount()).andReturn(idleTaskSlots);
    EasyMock.expect(workerTaskRunner.getLazyTaskSlotCount()).andReturn(idleTaskSlots2);

    replayAll();
    Assert.assertEquals(
        ImmutableMap.builder().putAll(idleTaskSlots).putAll(idleTaskSlots2).build(),
        runner.getLazyTaskSlotCount()
    );
    verifyAll();
  }
}
