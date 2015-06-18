/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord.autoscaling;

import io.druid.indexing.overlord.RemoteTaskRunner;
import io.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import io.druid.indexing.overlord.ZkWorker;

import java.util.Collection;

/**
 * The ResourceManagementStrategy decides if worker nodes should be provisioned or determined
 * based on the available tasks in the system and the state of the workers in the system.
 */
public interface ResourceManagementStrategy
{
  public boolean doProvision(RemoteTaskRunner runner);

  public boolean doTerminate(RemoteTaskRunner runner);

  public ScalingStats getStats();
}
