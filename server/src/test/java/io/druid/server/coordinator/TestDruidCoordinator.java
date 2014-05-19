package io.druid.server.coordinator;

import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.ServerInventoryView;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.common.config.JacksonConfigManager;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.db.DatabaseRuleManager;
import io.druid.db.DatabaseSegmentManager;
import io.druid.guice.annotations.Self;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ConcurrentMap;

/**
 */
public class TestDruidCoordinator extends DruidCoordinator
{
  public TestDruidCoordinator(
  )
  {
    super(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        ScheduledExecutors.createFactory(new Lifecycle()),
        null,
        null,
        null,
        null
    );
  }

  public TestDruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      DatabaseSegmentManager databaseSegmentManager,
      ServerInventoryView serverInventoryView,
      DatabaseRuleManager databaseRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self
  )
  {
    super(
        config,
        zkPaths,
        configManager,
        databaseSegmentManager,
        serverInventoryView,
        databaseRuleManager,
        curator,
        emitter,
        scheduledExecutorFactory,
        indexingServiceClient,
        taskMaster,
        serviceAnnouncer,
        self
    );
  }

  public TestDruidCoordinator(
      DruidCoordinatorConfig config,
      ZkPathsConfig zkPaths,
      JacksonConfigManager configManager,
      DatabaseSegmentManager databaseSegmentManager,
      ServerInventoryView serverInventoryView,
      DatabaseRuleManager databaseRuleManager,
      CuratorFramework curator,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      ServiceAnnouncer serviceAnnouncer,
      DruidNode self,
      ConcurrentMap<String, LoadQueuePeon> loadQueuePeonMap
  )
  {
    super(
        config,
        zkPaths,
        configManager,
        databaseSegmentManager,
        serverInventoryView,
        databaseRuleManager,
        curator,
        emitter,
        scheduledExecutorFactory,
        indexingServiceClient,
        taskMaster,
        serviceAnnouncer,
        self,
        loadQueuePeonMap
    );
  }

  @Override
  public boolean canPerformLeaderAction()
  {
    return true;
  }
}
