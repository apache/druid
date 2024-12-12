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

package org.apache.druid.server;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.utils.JvmUtils;

public class SubqueryGuardrailHelperProvider implements Provider<SubqueryGuardrailHelper>
{
  private final LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;
  private final ServerConfig serverConfig;
  private final QuerySchedulerConfig querySchedulerConfig;

  @Inject
  public SubqueryGuardrailHelperProvider(
      LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider,
      ServerConfig serverConfig,
      QuerySchedulerConfig querySchedulerConfig
  )
  {
    this.lookupExtractorFactoryContainerProvider = lookupExtractorFactoryContainerProvider;
    this.serverConfig = serverConfig;
    this.querySchedulerConfig = querySchedulerConfig;
  }

  @Override
  @LazySingleton
  public SubqueryGuardrailHelper get()
  {
    final int maxConcurrentQueries;

    if (querySchedulerConfig.getNumThreads() > 0) {
      maxConcurrentQueries = Math.min(
          querySchedulerConfig.getNumThreads(),
          serverConfig.getNumThreads()
      );
    } else {
      maxConcurrentQueries = serverConfig.getNumThreads();
    }

    return new SubqueryGuardrailHelper(
        lookupExtractorFactoryContainerProvider,
        JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(),
        maxConcurrentQueries
    );
  }
}
