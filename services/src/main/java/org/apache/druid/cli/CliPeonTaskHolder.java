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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.metrics.TaskHolder;

/**
 * TaskHolder implementation for peon processes.
 *
 * <p>This holder retrieves task information lazily via a {@link Provider} to avoid
 * circular dependencies during Guice initialization.</p>
 */
public class CliPeonTaskHolder implements TaskHolder
{
  private static final Logger log = new Logger(CliPeonTaskHolder.class);
  private Provider<Task> taskProvider;

  @Inject
  public CliPeonTaskHolder(
      Provider<Task> taskProvider
  )
  {
    log.info("GRRRONCE NewTaskPropertiesHolder with [%s]", taskProvider);
    this.taskProvider = taskProvider;
  }

  @Override
  public String getDataSource()
  {
    final Task task = taskProvider.get();
    log.info("GRRR NewTaskPropertiesHolder.getDataSource() task[%s]", task);
    if (task == null) {
      throw DruidException.defensive("blah");
    }
    return task.getDataSource();
  }

  @Override
  public String getTaskId()
  {
    final Task task = taskProvider.get();
    log.info("GRRR NewTaskPropertiesHolder.getDataSource() task[%s]", task);
    if (task == null) {
      throw DruidException.defensive("blah");
    }
    return task.getId();
  }
}
