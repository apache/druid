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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.DurableStorageConfigurationFault;
import org.apache.druid.msq.indexing.error.InsertTimeNullFault;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.druid.msq.statistics.KeyCollectorFactory;
import org.apache.druid.msq.statistics.KeyCollectorSnapshot;
import org.apache.druid.msq.statistics.KeyCollectorSnapshotDeserializerModule;
import org.apache.druid.msq.statistics.KeyCollectors;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.server.DruidNode;
import org.apache.druid.storage.StorageConnector;

import javax.annotation.Nullable;
import java.util.UUID;

public class MSQTasks
{
  /**
   * Message used by {@link #makeErrorReport} when no other message is known.
   */
  static final String GENERIC_QUERY_FAILED_MESSAGE = "Query failed";

  private static final String TASK_ID_PREFIX = "query-";

  /**
   * Returns a controller task ID given a SQL query id.
   */
  public static String controllerTaskId(@Nullable final String queryId)
  {
    return TASK_ID_PREFIX + (queryId == null ? UUID.randomUUID().toString() : queryId);
  }

  /**
   * Returns a worker task ID given a SQL query id.
   */
  public static String workerTaskId(final String controllerTaskId, final int workerNumber)
  {
    return StringUtils.format("%s-worker%d", controllerTaskId, workerNumber);
  }

  /**
   * If "Object" is a Long, returns it. Otherwise, throws an appropriate exception assuming this operation is
   * being done to read the primary timestamp (__time) as part of an INSERT.
   */
  public static long primaryTimestampFromObjectForInsert(final Object timestamp)
  {
    if (timestamp instanceof Long) {
      return (long) timestamp;
    } else if (timestamp == null) {
      throw new MSQException(InsertTimeNullFault.INSTANCE);
    } else {
      // Normally we expect the SQL layer to validate that __time for INSERT is a TIMESTAMP type, which would
      // be a long at execution time. So a nice user-friendly message isn't needed here: it would only happen
      // if the SQL layer is bypassed. Nice, friendly users wouldn't do that :)
      final UnknownFault fault =
          UnknownFault.forMessage(StringUtils.format("Incorrect type for [%s]", ColumnHolder.TIME_COLUMN_NAME));
      throw new MSQException(fault);
    }
  }

  /**
   * Returns a decorated copy of an ObjectMapper that knows how to deserialize the appropriate kind of
   * {@link KeyCollectorSnapshot}.
   */
  static ObjectMapper decorateObjectMapperForKeyCollectorSnapshot(
      final ObjectMapper mapper,
      final ClusterBy clusterBy,
      final boolean aggregate
  )
  {
    final KeyCollectorFactory<?, ?> keyCollectorFactory =
        KeyCollectors.makeStandardFactory(clusterBy, aggregate);

    final ObjectMapper mapperCopy = mapper.copy();
    mapperCopy.registerModule(new KeyCollectorSnapshotDeserializerModule(keyCollectorFactory));
    return mapperCopy;
  }

  /**
   * Returns the host:port from a {@link DruidNode}. Convenience method to make it easier to construct
   * {@link MSQErrorReport} instances.
   */
  @Nullable
  static String getHostFromSelfNode(@Nullable final DruidNode selfNode)
  {
    return selfNode != null ? selfNode.getHostAndPortToUse() : null;
  }

  static StorageConnector makeStorageConnector(final Injector injector)
  {
    try {
      return injector.getInstance(Key.get(StorageConnector.class, MultiStageQuery.class));
    }
    catch (Exception e) {
      throw new MSQException(new DurableStorageConfigurationFault(e.toString()));
    }
  }

  /**
   * Builds an error report from a possible controller error report and a possible worker error report. Both may be
   * null, in which case this function will return a report with {@link UnknownFault}.
   *
   * We only include a single {@link MSQErrorReport} in the task report, because it's important that a query have
   * a single {@link MSQFault} explaining why it failed. To aid debugging
   * in cases where we choose the controller error over the worker error, we'll log the worker error too, even though
   * it doesn't appear in the report.
   *
   * Logic: we prefer the controller exception unless it's {@link WorkerFailedFault}, {@link WorkerRpcFailedFault},
   * or {@link CanceledFault}. In these cases we prefer the worker error report. This ensures we get the best, most
   * useful exception even when the controller cancels worker tasks after a failure. (As tasks are canceled one by
   * one, worker -> worker and controller -> worker RPCs to the canceled tasks will fail. We want to ignore these
   * failed RPCs and get to the "true" error that started it all.)
   */
  static MSQErrorReport makeErrorReport(
      final String controllerTaskId,
      final String controllerHost,
      @Nullable MSQErrorReport controllerErrorReport,
      @Nullable MSQErrorReport workerErrorReport
  )
  {
    if (controllerErrorReport == null && workerErrorReport == null) {
      // Something went wrong, but we have no idea what.
      return MSQErrorReport.fromFault(
          controllerTaskId,
          controllerHost,
          null,
          UnknownFault.forMessage(GENERIC_QUERY_FAILED_MESSAGE)
      );
    } else if (controllerErrorReport == null) {
      // workerErrorReport is nonnull.
      return workerErrorReport;
    } else {
      // controllerErrorReport is nonnull.

      // Pick the "best" error if both are set. See the javadoc for the logic we use. In these situations, we
      // expect the caller to also log the other one. (There is no logging in _this_ method, because it's a helper
      // function, and it's best if helper functions run quietly.)
      if (workerErrorReport != null && (controllerErrorReport.getFault() instanceof WorkerFailedFault
                                        || controllerErrorReport.getFault() instanceof WorkerRpcFailedFault
                                        || controllerErrorReport.getFault() instanceof CanceledFault)) {
        return workerErrorReport;
      } else {
        return controllerErrorReport;
      }
    }
  }

  /**
   * Returns a string form of a {@link MSQErrorReport} suitable for logging.
   */
  static String errorReportToLogMessage(final MSQErrorReport errorReport)
  {
    final StringBuilder logMessage = new StringBuilder("Work failed");

    if (errorReport.getStageNumber() != null) {
      logMessage.append("; stage ").append(errorReport.getStageNumber());
    }

    logMessage.append("; task ").append(errorReport.getTaskId());

    if (errorReport.getHost() != null) {
      logMessage.append("; host ").append(errorReport.getHost());
    }

    logMessage.append(": ").append(errorReport.getFault().getCodeWithMessage());

    if (errorReport.getExceptionStackTrace() != null) {
      if (errorReport.getFault() instanceof UnknownFault) {
        // Log full stack trace for unknown faults.
        logMessage.append('\n').append(errorReport.getExceptionStackTrace());
      } else {
        // Log first line only (error class, message) for known faults, to avoid polluting logs.
        final String stackTrace = errorReport.getExceptionStackTrace();
        final int firstNewLine = stackTrace.indexOf('\n');

        logMessage.append(" (")
                  .append(firstNewLine > 0 ? stackTrace.substring(0, firstNewLine) : stackTrace)
                  .append(")");
      }
    }

    return logMessage.toString();
  }
}
