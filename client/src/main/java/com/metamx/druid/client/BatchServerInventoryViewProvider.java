package com.metamx.druid.client;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;

import javax.validation.constraints.NotNull;

/**
 */
public class BatchServerInventoryViewProvider implements ServerInventoryViewProvider
{
  @JacksonInject
  @NotNull
  private ZkPathsConfig zkPaths = null;

  @JacksonInject
  @NotNull
  private CuratorFramework curator = null;

  @JacksonInject
  @NotNull
  private ObjectMapper jsonMapper = null;

  @Override
  public ServerInventoryView get()
  {
    return new BatchServerInventoryView(zkPaths, curator, jsonMapper);
  }
}
