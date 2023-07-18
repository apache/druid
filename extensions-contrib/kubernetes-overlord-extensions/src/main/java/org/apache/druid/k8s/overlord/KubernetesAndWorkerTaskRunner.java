package org.apache.druid.k8s.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import io.vavr.collection.Iterator;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.k8s.overlord.KubernetesTaskRunner;
import org.apache.druid.tasklogs.TaskLogStreamer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class KubernetesAndWorkerTaskRunner implements TaskLogStreamer, WorkerTaskRunner {
  private final KubernetesTaskRunner kubernetesTaskRunner;
  private final WorkerTaskRunner workerTaskRunner;
  private final Boolean sendTasksToWorkerTaskRunner;

  public KubernetesAndWorkerTaskRunner(
      KubernetesTaskRunner kubernetesTaskRunner,
      WorkerTaskRunner workerTaskRunner,
      Boolean sendTasksToWorkerTaskRunner
  )
  {
    this.kubernetesTaskRunner = kubernetesTaskRunner;
    this.workerTaskRunner = workerTaskRunner;
    this.sendTasksToWorkerTaskRunner = sendTasksToWorkerTaskRunner;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore() {
    return Iterator.concat(kubernetesTaskRunner.restore(), kubernetesTaskRunner.restore()).collect(Collectors.toList());
  }

  @Override
  public void start() {
    kubernetesTaskRunner.start();
    workerTaskRunner.start();
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor) {
    kubernetesTaskRunner.registerListener(listener, executor);
    workerTaskRunner.registerListener(listener, executor);
  }

  @Override
  public void unregisterListener(String listenerId) {
    kubernetesTaskRunner.unregisterListener(listenerId);
    workerTaskRunner.unregisterListener(listenerId);
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task) {
    if (sendTasksToWorkerTaskRunner) {
      return workerTaskRunner.run(task);
    } else {
      return kubernetesTaskRunner.run(task);
    }
  }

  @Override
  public void shutdown(String taskid, String reason) {
    // Technically this is a no-op for tasks a runner does't know about.
    workerTaskRunner.shutdown(taskid, reason);
    kubernetesTaskRunner.shutdown(taskid, reason);
  }

  @Override
  public void stop() {
    kubernetesTaskRunner.stop();
    workerTaskRunner.stop();
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks() {
    return Iterator.concat(kubernetesTaskRunner.getRunningTasks(), workerTaskRunner.getRunningTasks()).collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks() {
    return Iterator.concat(kubernetesTaskRunner.getPendingTasks(), workerTaskRunner.getPendingTasks()).collect(Collectors.toList());

  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks() {
    return Iterator.concat(kubernetesTaskRunner.getKnownTasks(), workerTaskRunner.getKnownTasks()).collect(Collectors.toList());

  }

  @Override
  public Optional<ScalingStats> getScalingStats() {
    return workerTaskRunner.getScalingStats();
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount() {
    Map<String, Long> taskSlotCounts = Maps.newHashMap();
    taskSlotCounts.putAll(kubernetesTaskRunner.getTotalTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getTotalTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount() {
    Map<String, Long> taskSlotCounts = Maps.newHashMap();
    taskSlotCounts.putAll(kubernetesTaskRunner.getIdleTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getIdleTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount() {
    Map<String, Long> taskSlotCounts = Maps.newHashMap();
    taskSlotCounts.putAll(kubernetesTaskRunner.getUsedTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getUsedTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount() {
    Map<String, Long> taskSlotCounts = Maps.newHashMap();
    taskSlotCounts.putAll(kubernetesTaskRunner.getLazyTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getLazyTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount() {
    Map<String, Long> taskSlotCounts = Maps.newHashMap();
    taskSlotCounts.putAll(kubernetesTaskRunner.getBlacklistedTaskSlotCount());
    taskSlotCounts.putAll(workerTaskRunner.getBlacklistedTaskSlotCount());
    return taskSlotCounts;
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers() {
    return workerTaskRunner.getWorkers();
  }

  @Override
  public Collection<Worker> getLazyWorkers() {
    return workerTaskRunner.getLazyWorkers();
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxWorkers) {
    return workerTaskRunner.markWorkersLazy(isLazyWorker, maxWorkers);
  }

  @Override
  public WorkerTaskRunnerConfig getConfig() {
    return workerTaskRunner.getConfig();
  }

  @Override
  public Collection<Task> getPendingTaskPayloads() {
    return workerTaskRunner.getPendingTaskPayloads();
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskid, long offset) throws IOException {
    Optional<InputStream> kubernetesTaskLog = kubernetesTaskRunner.streamTaskLog(taskid, offset);
    if (kubernetesTaskLog.isPresent()) {
      return kubernetesTaskLog;
    } else if (workerTaskRunner instanceof TaskLogStreamer) {
      return ((TaskLogStreamer) workerTaskRunner).streamTaskLog(taskid, offset);
    }
    return Optional.absent();
  }
}
