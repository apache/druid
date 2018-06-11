/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.plumber;

import com.google.common.base.Supplier;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.segment.incremental.IncrementalIndexAddResult;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.realtime.FireDepartmentMetrics;

public class Plumbers
{
  private static final Logger log = new Logger(Plumbers.class);

  private Plumbers()
  {
    // No instantiation
  }

  public static void addNextRow(
      final Supplier<Committer> committerSupplier,
      final Firehose firehose,
      final Plumber plumber,
      final boolean reportParseExceptions,
      final FireDepartmentMetrics metrics
  )
  {
    final InputRow inputRow;
    try {
      inputRow = firehose.nextRow();
    }
    catch (ParseException e) {
      if (reportParseExceptions) {
        throw e;
      } else {
        log.debug(e, "Discarded row due to exception, considering unparseable.");
        metrics.incrementUnparseable();
        return;
      }
    }

    if (inputRow == null) {
      log.debug("Discarded null row, considering thrownAway.");
      metrics.incrementThrownAway();
      return;
    }

    final IncrementalIndexAddResult addResult;
    try {
      addResult = plumber.add(inputRow, committerSupplier);
    }
    catch (IndexSizeExceededException e) {
      // Shouldn't happen if this is only being called by a single thread.
      // plumber.add should be swapping out indexes before they fill up.
      throw new ISE(e, "WTF?! Index size exceeded, this shouldn't happen. Bad Plumber!");
    }

    if (addResult.getRowCount() == -1) {
      metrics.incrementThrownAway();
      log.debug("Discarded row[%s], considering thrownAway due to %s.", inputRow, addResult.getReasonOfNotAdded());
      return;
    }

    if (addResult.getRowCount() == -2) {
      metrics.incrementDedup();
      log.debug("Discarded row[%s], considering duplication.", inputRow);
      return;
    }

    metrics.incrementProcessed();
  }
}
