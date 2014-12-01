/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.overlord.autoscaling;

import io.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;

import java.util.Collection;

/**
 * The ResourceManagementStrategy decides if worker nodes should be provisioned or determined
 * based on the available tasks in the system and the state of the workers in the system.
 */
public interface ResourceManagementStrategy
{
  public boolean doProvision(Collection<RemoteTaskRunnerWorkItem> runningTasks, Collection<ZkWorker> zkWorkers);

  public boolean doTerminate(Collection<RemoteTaskRunnerWorkItem> runningTasks, Collection<ZkWorker> zkWorkers);

  public ScalingStats getStats();
}
