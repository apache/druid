package org.apache.druid.indexing.overlord.autoscaling;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ZkWorker;
import org.apache.druid.indexing.overlord.setup.CategoriedWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.FillCapacityWithCategorySpecWorkerSelectStrategy;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerCategorySpec;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class StrategyTestUtils
{
  public static final String MIN_VERSION = "2014-01-00T00:01:00Z";

  public static void setupAutoscaler(
      AutoScaler autoScaler,
      String category,
      int minWorkers,
      int maxWorkers,
      List<String> pendingTasks
  )
  {
    setupAutoscaler(autoScaler, category, minWorkers, pendingTasks);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(maxWorkers);
  }

  public static void setupAutoscaler(
      AutoScaler autoScaler,
      String category,
      int minWorkers,
      int maxWorkers,
      List<String> pendingTasks,
      int categoryTimes,
      int minWorkersTimes,
      int maxWorkersTimes,
      int pendingTasksTimes
  )
  {
    setupAutoscaler(autoScaler, category, minWorkers, pendingTasks, categoryTimes, minWorkersTimes, pendingTasksTimes);
    EasyMock.expect(autoScaler.getMaxNumWorkers()).andReturn(maxWorkers).times(maxWorkersTimes);
  }

  public static void setupAutoscaler(AutoScaler autoScaler, String category, int minWorkers, List<String> pendingTasks)
  {
    setupAutoscaler(autoScaler, category, minWorkers, pendingTasks, 4, 1, 1);
  }

  public static void setupAutoscaler(
      AutoScaler autoScaler,
      String category,
      int minWorkers,
      List<String> pendingTasks,
      int categoryTimes,
      int minWorkersTimes,
      int pendingTasksTimes
  )
  {
    EasyMock.expect(autoScaler.getMinNumWorkers()).andReturn(minWorkers).times(minWorkersTimes);
    EasyMock.expect(autoScaler.getCategory()).andReturn(category).times(categoryTimes);
    EasyMock.expect(autoScaler.ipToIdLookup(EasyMock.anyObject()))
            .andReturn(pendingTasks).times(pendingTasksTimes);
  }

  public static WorkerCategorySpec createWorkerCategorySpec(boolean isStrong)
  {
    Map<String, WorkerCategorySpec.CategoryConfig> categoryMap = new HashMap<>();
    return new WorkerCategorySpec(categoryMap, isStrong);
  }

  public static WorkerCategorySpec createWorkerCategorySpec(
      boolean isStrong,
      String taskType,
      String defaultCategory,
      String datasource,
      String category
  )
  {
    Map<String, String> categoryAffinity = new HashMap<>();
    categoryAffinity.put(datasource, category);
    WorkerCategorySpec.CategoryConfig categoryConfig = new WorkerCategorySpec.CategoryConfig(
        defaultCategory,
        categoryAffinity
    );
    Map<String, WorkerCategorySpec.CategoryConfig> categoryMap = new HashMap<>();
    categoryMap.put(taskType, categoryConfig);
    return new WorkerCategorySpec(categoryMap, isStrong);
  }

  public static WorkerCategorySpec createWorkerCategorySpec(
      boolean isStrong,
      String taskType1,
      WorkerCategorySpec.CategoryConfig categoryConfig1,
      String taskType2,
      WorkerCategorySpec.CategoryConfig categoryConfig2
  )
  {
    Map<String, WorkerCategorySpec.CategoryConfig> categoryMap = new HashMap<>();
    categoryMap.put(taskType1, categoryConfig1);
    categoryMap.put(taskType2, categoryConfig2);
    return new WorkerCategorySpec(categoryMap, isStrong);
  }

  public static AtomicReference<WorkerBehaviorConfig> createWorkerConfigRef(WorkerCategorySpec workerCategorySpec, List<AutoScaler> autoScalers)
  {
    return new AtomicReference<>(
        new CategoriedWorkerBehaviorConfig(
            new FillCapacityWithCategorySpecWorkerSelectStrategy(workerCategorySpec),
            autoScalers
        )
    );
  }

  public static class TestZkWorker extends ZkWorker
  {
    private final Task testTask;

    public TestZkWorker(
        Task testTask
    )
    {
      this(testTask, "http", "host", "ip", MIN_VERSION, 1, WorkerConfig.DEFAULT_CATEGORY);
    }

    public TestZkWorker(
        Task testTask,
        String category
    )
    {
      this(testTask, "http", "host", "ip", MIN_VERSION, 1, category);
    }

    public TestZkWorker(
        Task testTask,
        String scheme,
        String host,
        String ip,
        String version,
        int capacity,
        String category
    )
    {
      super(new Worker(scheme, host, ip, capacity, version, category), null, new DefaultObjectMapper());

      this.testTask = testTask;
    }

    @Override
    public Map<String, TaskAnnouncement> getRunningTasks()
    {
      if (testTask == null) {
        return new HashMap<>();
      }
      return ImmutableMap.of(
          testTask.getId(),
          TaskAnnouncement.create(
              testTask,
              TaskStatus.running(testTask.getId()),
              TaskLocation.unknown()
          )
      );
    }
  }

  public static class TestTask extends NoopTask
  {
    private final String type;

    public TestTask(
        String id,
        String groupId,
        String dataSource,
        long runTime,
        long isReadyTime,
        String isReadyResult,
        FirehoseFactory firehoseFactory,
        Map<String, Object> context,
        String type
    )
    {
      super(id, groupId, dataSource, runTime, isReadyTime, isReadyResult, firehoseFactory, context);
      this.type = type;
    }

    public static TestTask create(String taskType, String dataSource)
    {
      return new TestTask(null, null, dataSource, 0, 0, null, null, null, taskType);
    }

    public static TestTask create(String id, String taskType, String dataSource)
    {
      return new TestTask(id, null, dataSource, 0, 0, null, null, null, taskType);
    }

    @Override
    public String getType()
    {
      return type;
    }
  }
}
