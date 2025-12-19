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

package org.apache.druid.msq.exec;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyClusteredByColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;

public class QueryValidator
{
  /**
   * Validate that a {@link QueryDefinition} falls within the {@link Limits#MAX_FRAME_COLUMNS} and
   * {@link Limits#MAX_WORKERS} limits.
   */
  public static void validateQueryDef(final QueryDefinition queryDef)
  {
    for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
      final int numColumns = stageDef.getSignature().size();

      if (numColumns > Limits.MAX_FRAME_COLUMNS) {
        throw new MSQException(new TooManyColumnsFault(numColumns, Limits.MAX_FRAME_COLUMNS));
      }

      final int numClusteredByColumns = stageDef.getClusterBy().getColumns().size();
      if (numClusteredByColumns > Limits.MAX_CLUSTERED_BY_COLUMNS) {
        throw new MSQException(
            new TooManyClusteredByColumnsFault(
                numClusteredByColumns,
                Limits.MAX_CLUSTERED_BY_COLUMNS,
                stageDef.getStageNumber()
            )
        );
      }

      final int numWorkers = stageDef.getMaxWorkerCount();
      if (numWorkers > Limits.MAX_WORKERS) {
        throw new MSQException(new TooManyWorkersFault(numWorkers, Limits.MAX_WORKERS));
      } else if (numWorkers <= 0) {
        throw new ISE("Number of workers must be greater than 0");
      }
    }
  }
}
