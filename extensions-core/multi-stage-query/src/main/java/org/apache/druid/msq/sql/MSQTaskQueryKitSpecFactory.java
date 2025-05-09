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

package org.apache.druid.msq.sql;

import com.google.inject.Inject;
import org.apache.druid.msq.exec.QueryKitSpecFactory;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;

public class MSQTaskQueryKitSpecFactory implements QueryKitSpecFactory
{
  private DruidProcessingConfig processingConfig;

  @Inject
  public MSQTaskQueryKitSpecFactory(DruidProcessingConfig processingConfig)
  {
    this.processingConfig = processingConfig;
  }

  @Override
  public QueryKitSpec makeQueryKitSpec(
      QueryKit<Query<?>> queryKit,
      String queryId,
      MSQTuningConfig tuningConfig,
      QueryContext queryContext)
  {
    return new QueryKitSpec(
        queryKit,
        queryId,
        tuningConfig.getMaxNumWorkers(),
        tuningConfig.getMaxNumWorkers(),

        // Assume tasks are symmetric: workers have the same number of processors available as a controller.
        // Create one partition per processor per task, for maximum parallelism.
        MultiStageQueryContext.getTargetPartitionsPerWorkerWithDefault(
            queryContext,
            processingConfig.getNumThreads()
        )
    );
  }

}
