/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.metrics.LoadSpecHolder;

/**
 * LoadSpecHolder implementation for peon processes.
 *
 * <p>This holder retrieves load specifications lazily via a {@link Provider} to avoid
 * circular dependencies during Guice initialization.</p>
 */
public class CliPeonLoadSpecHolder implements LoadSpecHolder
{
  private Provider<Task> taskProvider;

  @Inject
  public CliPeonLoadSpecHolder(
      Provider<Task> taskProvider
  )
  {
    this.taskProvider = taskProvider;
  }


  @Override
  public LookupLoadingSpec getLookupLoadingSpec()
  {
    final Task task = taskProvider.get();
    if (task == null) {
      throw DruidException.defensive("task is null for taskProvider[%s]?!", taskProvider);
    }
    return task.getLookupLoadingSpec();
  }

  @Override
  public BroadcastDatasourceLoadingSpec getBroadcastDatasourceLoadingSpec()
  {
    final Task task = taskProvider.get();
    if (task == null) {
      throw DruidException.defensive("task is null for taskProvider[%s]?!", taskProvider);
    }
    return task.getBroadcastDatasourceLoadingSpec();
  }
}
