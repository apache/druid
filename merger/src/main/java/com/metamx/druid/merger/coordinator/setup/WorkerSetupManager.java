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

package com.metamx.druid.merger.coordinator.setup;

import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.druid.config.JacksonConfigManager;

import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class WorkerSetupManager
{
  private static final String WORKER_SETUP_KEY = "worker.setup";

  private final JacksonConfigManager configManager;

  private volatile AtomicReference<WorkerSetupData> workerSetupData = new AtomicReference<WorkerSetupData>(null);

  public WorkerSetupManager(
      JacksonConfigManager configManager
  )
  {
    this.configManager = configManager;
  }

  @LifecycleStart
  public void start()
  {
    workerSetupData = configManager.watch(WORKER_SETUP_KEY, WorkerSetupData.class);
  }

  public WorkerSetupData getWorkerSetupData()
  {
    return workerSetupData.get();
  }

  public boolean setWorkerSetupData(final WorkerSetupData value)
  {
    return configManager.set(WORKER_SETUP_KEY, value);
  }
}
