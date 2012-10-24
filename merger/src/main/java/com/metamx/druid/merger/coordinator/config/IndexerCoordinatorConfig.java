package com.metamx.druid.merger.coordinator.config;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.metamx.druid.merger.common.task.Task;
import org.skife.config.Config;
import org.skife.config.Default;

import java.io.File;
import java.util.Set;

/**
 */
public abstract class IndexerCoordinatorConfig
{
  private volatile Set<String> whitelistDatasources = null;

  @Config("druid.host")
  public abstract String getServerName();

  @Config("druid.zk.paths.indexer.leaderLatchPath")
  public abstract String getLeaderLatchPath();

  @Config("druid.merger.threads")
  @Default("1")
  public abstract int getNumLocalThreads();

  @Config("druid.merger.runner")
  @Default("remote")
  public abstract String getRunnerImpl();

  @Config("druid.merger.storage")
  @Default("local")
  public abstract String getStorageImpl();

  @Config("druid.merger.taskDir")
  public abstract File getBaseTaskDir();

  @Config("druid.merger.whitelist.enabled")
  @Default("false")
  public abstract boolean isWhitelistEnabled();

  @Config("druid.merger.whitelist.datasources")
  @Default("")
  public abstract String getWhitelistDatasourcesString();

  public File getTaskDir(final Task task) {
    return new File(getBaseTaskDir(), task.getId());
  }

  public Set<String> getWhitelistDatasources()
  {
    if(whitelistDatasources == null) {
      synchronized (this) {
        if(whitelistDatasources == null) {
          whitelistDatasources = ImmutableSet.copyOf(Splitter.on(",").split(getWhitelistDatasourcesString()));
        }
      }
    }

    return whitelistDatasources;
  }

  @Config("druid.merger.rowFlushBoundary")
  @Default("500000")
  public abstract long getRowFlushBoundary();
}
