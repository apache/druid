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

import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyColumnsFault;
import org.apache.druid.msq.indexing.error.TooManyInputFilesFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkOrder;

import java.math.RoundingMode;

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

      final int numWorkers = stageDef.getMaxWorkerCount();
      if (numWorkers > Limits.MAX_WORKERS) {
        throw new MSQException(new TooManyWorkersFault(numWorkers, Limits.MAX_WORKERS));
      } else if (numWorkers <= 0) {
        throw new ISE("Number of workers must be greater than 0");
      }
    }
  }

  /**
   * Validate that a {@link WorkOrder} falls within the {@link Limits#MAX_INPUT_FILES_PER_WORKER} limit.
   */
  public static void validateWorkOrder(final WorkOrder order)
  {
    final int numInputFiles = Ints.checkedCast(order.getInputs().stream().mapToLong(InputSlice::fileCount).sum());

    if (numInputFiles > Limits.MAX_INPUT_FILES_PER_WORKER) {
      throw new MSQException(
          new TooManyInputFilesFault(
              numInputFiles,
              Limits.MAX_INPUT_FILES_PER_WORKER,
              IntMath.divide(numInputFiles, Limits.MAX_INPUT_FILES_PER_WORKER, RoundingMode.CEILING)
          )
      );
    }
  }
}
