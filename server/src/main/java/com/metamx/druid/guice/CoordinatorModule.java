package com.metamx.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.druid.client.ServerInventoryViewConfig;
import com.metamx.druid.client.indexing.IndexingServiceClient;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.db.DatabaseRuleManagerConfig;
import com.metamx.druid.db.DatabaseRuleManagerProvider;
import com.metamx.druid.db.DatabaseSegmentManager;
import com.metamx.druid.db.DatabaseSegmentManagerConfig;
import com.metamx.druid.db.DatabaseSegmentManagerProvider;
import com.metamx.druid.http.MasterRedirectInfo;
import com.metamx.druid.http.RedirectFilter;
import com.metamx.druid.http.RedirectInfo;
import com.metamx.druid.http.RedirectServlet;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterConfig;
import com.metamx.druid.master.LoadQueueTaskMaster;
import org.apache.curator.framework.CuratorFramework;

/**
 */
public class CoordinatorModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, DruidMasterConfig.class);
    ConfigProvider.bind(binder, ServerInventoryViewConfig.class);

    JsonConfigProvider.bind(binder, "druid.manager.segment", DatabaseSegmentManagerConfig.class);
    JsonConfigProvider.bind(binder, "druid.manager.rules", DatabaseRuleManagerConfig.class);

    binder.bind(RedirectServlet.class).in(LazySingleton.class);
    binder.bind(RedirectFilter.class).in(LazySingleton.class);

    binder.bind(DatabaseSegmentManager.class)
          .toProvider(DatabaseSegmentManagerProvider.class)
          .in(ManageLifecycle.class);

    binder.bind(DatabaseRuleManager.class)
          .toProvider(DatabaseRuleManagerProvider.class)
          .in(ManageLifecycle.class);

    binder.bind(IndexingServiceClient.class).in(LazySingleton.class);

    binder.bind(RedirectInfo.class).to(MasterRedirectInfo.class).in(LazySingleton.class);

    binder.bind(DruidMaster.class);
  }

  @Provides @LazySingleton
  public LoadQueueTaskMaster getLoadQueueTaskMaster(
      CuratorFramework curator, ObjectMapper jsonMapper, ScheduledExecutorFactory factory, DruidMasterConfig config
  )
  {
    return new LoadQueueTaskMaster(curator, jsonMapper, factory.create(1, "Master-PeonExec--%d"), config);
  }
}
