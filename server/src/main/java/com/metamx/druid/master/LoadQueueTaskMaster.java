/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides LoadQueuePeons
 */
public class LoadQueueTaskMaster
{
  private final CuratorFramework curator;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService peonExec;
  private final DruidMasterConfig config;

  public LoadQueueTaskMaster(
      CuratorFramework curator,
      ObjectMapper jsonMapper,
      ScheduledExecutorService peonExec,
      DruidMasterConfig config
  )
  {
    this.curator = curator;
    this.jsonMapper = jsonMapper;
    this.peonExec = peonExec;
    this.config = config;
  }

  public LoadQueuePeon giveMePeon(String basePath)
  {
    return new LoadQueuePeon(curator, basePath, jsonMapper, peonExec, config);
  }
}
