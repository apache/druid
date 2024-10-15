package org.apache.druid.indexing.batch;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.discovery.BrokerClient;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * 1. Multiple datasources -- 2 supervisors targeting foo and 1 supervisor targeting bar
 */
public class ScheduledBatchSchedulerTest
{
  private static final ObjectMapper OBJECT_MAPPER;

  static {
    OBJECT_MAPPER = new DefaultObjectMapper();
    OBJECT_MAPPER.registerModules(new IndexingServiceTuningConfigModule().getJacksonModules());
    OBJECT_MAPPER.setInjectableValues(
        new InjectableValues
            .Std()
            .addValue(
                SegmentCacheManagerFactory.class,
                new SegmentCacheManagerFactory(TestIndex.INDEX_IO, OBJECT_MAPPER)
            )
    );
  }

  private TaskMaster taskMaster;
  private BlockingExecutorService executor;
  private BrokerClient brokerClient;
  private StubServiceEmitter serviceEmitter;

  private ScheduledBatchScheduler scheduler;

  @Before
  public void setUp()
  {
    brokerClient = Mockito.mock(BrokerClient.class);
    taskMaster = new TaskMaster(null, null);
    taskMaster.becomeLeader(Mockito.mock(TaskRunner.class), Mockito.mock(TaskQueue.class));
    executor = new BlockingExecutorService("test");
    serviceEmitter = new StubServiceEmitter();
    initScheduler();
  }

  private void initScheduler()
  {
    scheduler = new ScheduledBatchScheduler(
        taskMaster,
        (nameFormat, numThreads) -> new WrappingScheduledExecutorService("test", executor, false),
        serviceEmitter,
        OBJECT_MAPPER,
        null
    );
  }

  @Test
  public void testStartStop()
  {
    // Nothing to validate here. We can verify the task runner callback is registered, but we're using a mock now...
    scheduler.start();
    scheduler.stop();

    scheduler.start();
    scheduler.stop();
  }

  @Test
  public void testStartStopSchedulingSupervisor()
  {
    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("*/30 * * * * ?"), sqlQuery);
    BatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    Assert.assertEquals("Not run", snapshot.getPreviousTaskExecutionTime());

    scheduler.stopScheduledIngestion("foo");
    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("*/10 * * * * ?"), sqlQuery);
    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());

    scheduler.stop();
    Assert.assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsNull() throws Exception
  {
    Request request = Mockito.mock(Request.class);
    Mockito.when(brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/task/"))
//        .thenReturn(expectedRequest);
        .thenReturn(request);
    Mockito.when(brokerClient.sendQuery(request)).thenReturn(null);

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("* * * * * ?"), sqlQuery);
    BatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    Assert.assertEquals("Not run", snapshot.getPreviousTaskExecutionTime());

    Thread.sleep(5000);

    scheduler.stopScheduledIngestion("foo");
    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.stop();
    Assert.assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStartStopSchedulingSupervisorWhenResponseIsValid() throws Exception
  {
    final Request request = Mockito.mock(Request.class);
    Mockito.when(brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/task/"))
           .thenReturn(request);
    final SqlTaskStatus expectedTaskStatus = new SqlTaskStatus("taskId1", TaskState.SUCCESS, null);
    Mockito.when(brokerClient.sendQuery(request)).thenReturn(OBJECT_MAPPER.writeValueAsString(expectedTaskStatus));

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("* * * * * ?"), sqlQuery);
    BatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    Assert.assertEquals("Not run", snapshot.getPreviousTaskExecutionTime());

    Thread.sleep(2000);
    executor.finishNextPendingTask();
    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId())), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    Assert.assertNotEquals("Not run", snapshot.getPreviousTaskExecutionTime());

    scheduler.stopScheduledIngestion("foo");

    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(expectedTaskStatus.getTaskId(), TaskStatus.success(expectedTaskStatus.getTaskId())), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.stop();
    Assert.assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStartStopSchedulingMultipleSupervisors() throws Exception
  {
    final Request request = Mockito.mock(Request.class);
    Mockito.when(brokerClient.makeRequest(HttpMethod.POST, "/druid/v2/sql/task/"))
           .thenReturn(request);
    final SqlTaskStatus expectedTask1 = new SqlTaskStatus("taskId1", TaskState.SUCCESS, null);
    final SqlTaskStatus expectedTask2 = new SqlTaskStatus("taskId2", TaskState.RUNNING, null);
    Mockito.when(brokerClient.sendQuery(request))
           .thenReturn(OBJECT_MAPPER.writeValueAsString(expectedTask1))
           .thenReturn(OBJECT_MAPPER.writeValueAsString(expectedTask2));

    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("* * * * * ?"), sqlQuery);
    BatchSupervisorSnapshot snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    Assert.assertEquals("Not run", snapshot.getPreviousTaskExecutionTime());

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(expectedTask1.getTaskId(), TaskStatus.success(expectedTask1.getTaskId())), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING, snapshot.getStatus());
    Assert.assertNotEquals("Not run", snapshot.getPreviousTaskExecutionTime());

    Thread.sleep(1200);
    executor.finishNextPendingTask();
    scheduler.stopScheduledIngestion("foo");

    snapshot = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(snapshot);
    Assert.assertEquals("foo", snapshot.getSupervisorId());
    Assert.assertEquals(ImmutableMap.of(expectedTask2.getTaskId(), TaskStatus.running(expectedTask2.getTaskId())), snapshot.getActiveTasks());
    Assert.assertEquals(ImmutableMap.of(expectedTask1.getTaskId(), TaskStatus.success(expectedTask1.getTaskId())), snapshot.getCompletedTasks());
    Assert.assertEquals(BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN, snapshot.getStatus());

    scheduler.stop();
    Assert.assertNull(scheduler.getSchedulerSnapshot("foo"));
  }

  @Test
  public void testStopScheduling()
  {
    final SqlQuery sqlQuery = new SqlQuery(
        "SELECT ?",
        ResultFormat.ARRAY,
        true,
        true,
        true,
        ImmutableMap.of("useCache", false),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );

    scheduler.start();
    scheduler.startScheduledIngestion("foo", new QuartzCronSchedulerConfig("*/30 * * * * ?"), sqlQuery);
    final BatchSupervisorSnapshot state = scheduler.getSchedulerSnapshot("foo");
    Assert.assertNotNull(state);
  }
}