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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.lookup.LookupModule;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.druid.server.lookup.cache.LookupLoadingSpec;
import org.apache.druid.server.metrics.LoadSpecHolder;
import org.apache.druid.server.metrics.MetricsModule;

/**
 * LoadSpecHolder implementation for {@code CliPeon} processes.
 *
 * <p>This holder retrieves load specifications lazily via a {@link Provider} to avoid cyclic dependencies
 * during Guice initialization. A {@link Task} may include a {@link TransformSpec}, an {@link AggregatorFactory},
 * or an {@link AbstractMonitor}, all of which require modules such as {@link LookupModule} or {@link MetricsModule}.
 * Since those modules depend on this holder, using a Provider breaks the dependency cycle by binding it lazily.
 * </p>
 */
public class PeonLoadSpecHolder implements LoadSpecHolder
{
  private final Provider<Task> taskProvider;

  @Inject
  public PeonLoadSpecHolder(final Provider<Task> taskProvider)
  {
    this.taskProvider = taskProvider;
  }

  @Override
  public LookupLoadingSpec getLookupLoadingSpec()
  {
    return taskProvider.get().getLookupLoadingSpec();
  }

  @Override
  public BroadcastDatasourceLoadingSpec getBroadcastDatasourceLoadingSpec()
  {
    return taskProvider.get().getBroadcastDatasourceLoadingSpec();
  }
}
