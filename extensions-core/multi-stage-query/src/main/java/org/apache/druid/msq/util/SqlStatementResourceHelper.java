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


package org.apache.druid.msq.util;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.TaskReportMSQDestination;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqlStatementResourceHelper
{
  public static Optional<List<ColumnNameAndTypes>> getSignature(
      MSQControllerTask msqControllerTask
  )
  {
    // only populate signature for select q's
    if (msqControllerTask.getQuerySpec().getDestination().getClass() == TaskReportMSQDestination.class) {
      ColumnMappings columnMappings = msqControllerTask.getQuerySpec().getColumnMappings();
      List<SqlTypeName> sqlTypeNames = msqControllerTask.getSqlTypeNames();
      if (sqlTypeNames == null || sqlTypeNames.size() != columnMappings.size()) {
        return Optional.empty();
      }
      List<ColumnType> nativeTypeNames = msqControllerTask.getNativeTypeNames();
      if (nativeTypeNames == null || nativeTypeNames.size() != columnMappings.size()) {
        return Optional.empty();
      }
      List<ColumnNameAndTypes> signature = new ArrayList<>(columnMappings.size());
      int index = 0;
      for (String colName : columnMappings.getOutputColumnNames()) {
        signature.add(new ColumnNameAndTypes(
            colName,
            sqlTypeNames.get(index).getName(),
            nativeTypeNames.get(index).asTypeString()
        ));
        index++;
      }
      return Optional.of(signature);
    }
    return Optional.empty();
  }


  public static void isMSQPayload(TaskPayloadResponse taskPayloadResponse, String queryId) throws DruidException
  {
    if (taskPayloadResponse == null || taskPayloadResponse.getPayload() == null) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] not found", queryId);
    }

    if (MSQControllerTask.class != taskPayloadResponse.getPayload().getClass()) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] not found", queryId);
    }
  }

  public static SqlStatementState getSqlStatementState(TaskStatusPlus taskStatusPlus)
  {
    TaskState state = taskStatusPlus.getStatusCode();
    if (state == null) {
      return SqlStatementState.ACCEPTED;
    }

    switch (state) {
      case FAILED:
        return SqlStatementState.FAILED;
      case RUNNING:
        if (TaskLocation.unknown().equals(taskStatusPlus.getLocation())) {
          return SqlStatementState.ACCEPTED;
        } else {
          return SqlStatementState.RUNNING;
        }
      case SUCCESS:
        return SqlStatementState.SUCCESS;
      default:
        throw new ISE("Unrecognized state[%s] found.", state);
    }
  }

  @SuppressWarnings("unchecked")


  public static long getLastIndex(Long numberOfRows, long start)
  {
    final long last;
    if (numberOfRows == null) {
      last = Long.MAX_VALUE;
    } else {
      long finalIndex;
      try {
        finalIndex = Math.addExact(start, numberOfRows);
      }
      catch (ArithmeticException e) {
        finalIndex = Long.MAX_VALUE;
      }
      last = finalIndex;
    }
    return last;
  }

  public static Optional<Pair<Long, Long>> getRowsAndSizeFromPayload(Map<String, Object> payload, boolean isSelectQuery)
  {
    List stages = getList(payload, "stages");
    if (stages == null || stages.isEmpty()) {
      return Optional.empty();
    } else {
      int maxStage = stages.size() - 1; // Last stage output is the total number of rows returned to the end user.
      Map<String, Object> counterMap = getMap(getMap(payload, "counters"), String.valueOf(maxStage));
      long rows = -1L;
      long sizeInBytes = -1L;
      if (counterMap == null) {
        return Optional.empty();
      }
      for (Map.Entry<String, Object> worker : counterMap.entrySet()) {
        Object workerChannels = worker.getValue();
        if (workerChannels == null || !(workerChannels instanceof Map)) {
          return Optional.empty();
        }
        if (isSelectQuery) {
          Object output = ((Map<?, ?>) workerChannels).get("output");
          if (output != null && output instanceof Map) {
            List<Integer> rowsPerChannel = (List<Integer>) ((Map<String, Object>) output).get("rows");
            List<Integer> bytesPerChannel = (List<Integer>) ((Map<String, Object>) output).get("bytes");
            for (Integer row : rowsPerChannel) {
              rows = rows + row;
            }
            for (Integer bytes : bytesPerChannel) {
              sizeInBytes = sizeInBytes + bytes;
            }
          }
        } else {
          Object output = ((Map<?, ?>) workerChannels).get("segmentGenerationProgress");
          if (output != null && output instanceof Map) {
            rows += (Integer) ((Map<String, Object>) output).get("rowsPushed");
          }
        }
      }

      return Optional.of(new Pair<>(rows == -1L ? null : rows + 1, sizeInBytes == -1L ? null : sizeInBytes + 1));
    }
  }


  public static Optional<SqlStatementResult> getExceptionPayload(
      String queryId,
      TaskStatusResponse taskResponse,
      TaskStatusPlus statusPlus,
      SqlStatementState sqlStatementState,
      Map<String, Object> msqPayload
  )
  {
    Map<String, Object> exceptionDetails = getQueryExceptionDetails(getPayload(msqPayload));
    Map<String, Object> exception = getMap(exceptionDetails, "error");
    if (exceptionDetails == null || exception == null) {
      return Optional.of(new SqlStatementResult(
          queryId,
          sqlStatementState,
          taskResponse.getStatus().getCreatedTime(),
          null,
          taskResponse.getStatus().getDuration(),
          null,
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build(taskResponse.getStatus().getErrorMsg()).toErrorResponse()
      ));
    }

    final String errorMessage = String.valueOf(exception.getOrDefault("errorMessage", statusPlus.getErrorMsg()));
    exception.remove("errorMessage");
    String errorCode = String.valueOf(exception.getOrDefault("errorCode", "unknown"));
    exception.remove("errorCode");
    Map<String, String> stringException = new HashMap<>();
    for (Map.Entry<String, Object> exceptionKeys : exception.entrySet()) {
      stringException.put(exceptionKeys.getKey(), String.valueOf(exceptionKeys.getValue()));
    }
    return Optional.of(new SqlStatementResult(
        queryId,
        sqlStatementState,
        taskResponse.getStatus().getCreatedTime(),
        null,
        taskResponse.getStatus().getDuration(),
        null,
        DruidException.fromFailure(new DruidException.Failure(errorCode)
        {
          @Override
          protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
          {
            DruidException ex = bob.forPersona(DruidException.Persona.USER)
                                   .ofCategory(DruidException.Category.UNCATEGORIZED)
                                   .build(errorMessage);
            ex.withContext(stringException);
            return ex;
          }
        }).toErrorResponse()
    ));
  }

  public static Map<String, Object> getQueryExceptionDetails(Map<String, Object> payload)
  {
    return getMap(getMap(payload, "status"), "errorReport");
  }

  public static Map<String, Object> getMap(Map<String, Object> map, String key)
  {
    if (map == null) {
      return null;
    }
    return (Map<String, Object>) map.get(key);
  }

  @SuppressWarnings("rawtypes")
  public static List getList(Map<String, Object> map, String key)
  {
    if (map == null) {
      return null;
    }
    return (List) map.get(key);
  }

  /**
   * Get results from report
   */
  @SuppressWarnings("unchecked")
  public static Optional<List<Object>> getResults(Map<String, Object> payload)
  {
    Map<String, Object> resultsHolder = getMap(payload, "results");

    if (resultsHolder == null) {
      return Optional.empty();
    }

    List<Object> data = (List<Object>) resultsHolder.get("results");
    List<Object> rows = new ArrayList<>();
    if (data != null) {
      rows.addAll(data);
    }
    return Optional.of(rows);
  }

  public static Map<String, Object> getPayload(Map<String, Object> results)
  {
    Map<String, Object> msqReport = getMap(results, "multiStageQuery");
    Map<String, Object> payload = getMap(msqReport, "payload");
    return payload;
  }
}
