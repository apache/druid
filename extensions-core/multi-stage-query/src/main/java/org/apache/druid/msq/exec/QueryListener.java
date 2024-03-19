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

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.msq.indexing.report.MSQResultsReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Object passed to {@link Controller#run(QueryListener)} to enable retrieval of results, status, counters, etc.
 */
public interface QueryListener
{
  /**
   * Whether this listener is meant to receive results.
   */
  boolean readResults();

  /**
   * Called when results start coming in.
   *
   * @param signature    signature of results
   * @param sqlTypeNames SQL type names of results; same length as the signature
   */
  void onResultsStart(
      List<MSQResultsReport.ColumnAndType> signature,
      @Nullable List<SqlTypeName> sqlTypeNames
  );

  /**
   * Called for each result row. Follows a call to {@link #onResultsStart(List, List)}.
   *
   * @param row result row
   *
   * @return whether the controller should keep reading results
   */
  boolean onResultRow(Object[] row);

  /**
   * Called after the last result has been delivered by {@link #onResultRow(Object[])}. Only called if results are
   * actually complete. If results are truncated due to {@link #readResults()} or {@link #onResultRow(Object[])}
   * returning false, this method is not called.
   */
  void onResultsComplete();

  /**
   * Called when the query is complete and a report is available. After this method is called, no other methods
   * will be called. The report will not include a {@link MSQResultsReport}.
   */
  void onQueryComplete(MSQTaskReportPayload report);
}
