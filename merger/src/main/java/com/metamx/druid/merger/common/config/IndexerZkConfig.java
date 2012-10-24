package com.metamx.druid.merger.common.config;

import org.skife.config.Config;

/**
 */
public abstract class IndexerZkConfig
{
  @Config("druid.zk.paths.indexer.announcementsPath")
  public abstract String getAnnouncementPath();

  @Config("druid.zk.paths.indexer.tasksPath")
  public abstract String getTaskPath();

  @Config("druid.zk.paths.indexer.statusPath")
  public abstract String getStatusPath();
}
