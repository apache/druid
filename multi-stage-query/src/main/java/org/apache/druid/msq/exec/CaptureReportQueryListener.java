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

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import javax.annotation.Nullable;

/**
 * A {@link QueryListener} wrapper that captures the report from {@link #onQueryComplete(MSQTaskReportPayload)}.
 */
public class CaptureReportQueryListener implements QueryListener
{
  private final QueryListener delegate;

  @Nullable
  private volatile MSQTaskReportPayload report;

  public CaptureReportQueryListener(final QueryListener delegate)
  {
    this.delegate = delegate;
  }

  /**
   * Whether this listener has captured a report. Will be true if the query has completed, false otherwise.
   */
  public boolean hasReport()
  {
    return report != null;
  }

  /**
   * Retrieves the report. Can only be called once the query is complete.
   */
  public MSQTaskReportPayload getReport()
  {
    if (report == null) {
      throw DruidException.defensive("Query not complete, cannot call getReport()");
    }

    return report;
  }

  @Override
  public boolean readResults()
  {
    return delegate.readResults();
  }

  @Override
  public void onResultsStart(final FrameReader frameReader)
  {
    delegate.onResultsStart(frameReader);
  }

  @Override
  public boolean onResultBatch(RowsAndColumns rac)
  {
    return delegate.onResultBatch(rac);
  }

  @Override
  public void onResultsComplete()
  {
    delegate.onResultsComplete();
  }

  @Override
  public void onQueryComplete(final MSQTaskReportPayload report)
  {
    this.report = report;
    delegate.onQueryComplete(report);
  }
}
