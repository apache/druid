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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.write.InvalidNullByteException;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.groupby.epinephelinae.UnexpectedMultiValueDimensionException;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import javax.annotation.Nullable;
import java.util.Objects;

public class MSQErrorReport
{
  private final String taskId;
  @Nullable
  private final String host;
  @Nullable
  private final Integer stageNumber;
  private final MSQFault error;
  @Nullable
  private final String exceptionStackTrace;

  @JsonCreator
  MSQErrorReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("host") @Nullable final String host,
      @JsonProperty("stageNumber") @Nullable final Integer stageNumber,
      @JsonProperty("error") final MSQFault fault,
      @JsonProperty("exceptionStackTrace") @Nullable final String exceptionStackTrace
  )
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.host = host;
    this.stageNumber = stageNumber;
    this.error = Preconditions.checkNotNull(fault, "error");
    this.exceptionStackTrace = exceptionStackTrace;
  }

  public static MSQErrorReport fromFault(
      final String taskId,
      @Nullable final String host,
      @Nullable final Integer stageNumber,
      final MSQFault fault
  )
  {
    return new MSQErrorReport(taskId, host, stageNumber, fault, null);
  }

  public static MSQErrorReport fromException(
      final String taskId,
      @Nullable final String host,
      @Nullable final Integer stageNumber,
      final Throwable e
  )
  {
    return fromException(taskId, host, stageNumber, e, null);
  }

  public static MSQErrorReport fromException(
      final String taskId,
      @Nullable final String host,
      @Nullable final Integer stageNumber,
      final Throwable e,
      @Nullable final ColumnMappings columnMappings
  )
  {
    return new MSQErrorReport(
        taskId,
        host,
        stageNumber,
        getFaultFromException(e, columnMappings),
        Throwables.getStackTraceAsString(e)
    );
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getHost()
  {
    return host;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getStageNumber()
  {
    return stageNumber;
  }

  @JsonProperty("error")
  public MSQFault getFault()
  {
    return error;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExceptionStackTrace()
  {
    return exceptionStackTrace;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MSQErrorReport that = (MSQErrorReport) o;
    return Objects.equals(taskId, that.taskId)
           && Objects.equals(host, that.host)
           && Objects.equals(stageNumber, that.stageNumber)
           && Objects.equals(error, that.error)
           && Objects.equals(exceptionStackTrace, that.exceptionStackTrace);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, host, error, exceptionStackTrace);
  }

  @Override
  public String toString()
  {
    return "MSQErrorReport{" +
           "taskId='" + taskId + '\'' +
           ", host='" + host + '\'' +
           ", stageNumber=" + stageNumber +
           ", error=" + error +
           ", exceptionStackTrace='" + exceptionStackTrace + '\'' +
           '}';
  }

  public static MSQFault getFaultFromException(@Nullable final Throwable e)
  {
    return getFaultFromException(e, null);
  }

  /**
   * Magical code that extracts a useful fault from an exception, even if that exception is not necessarily a
   * {@link MSQException}. This method walks through the causal chain, and also "knows" about various exception
   * types thrown by other Druid code.
   */
  public static MSQFault getFaultFromException(@Nullable final Throwable e, @Nullable final ColumnMappings columnMappings)
  {
    // Unwrap exception wrappers to find an underlying fault. The assumption here is that the topmost recognizable
    // exception should be used to generate the fault code for the entire report.

    Throwable cause = e;

    // This method will grow as we try to add more faults and exceptions
    // One way of handling this would be to extend the faults to have a method like
    // public MSQFault fromException(@Nullable Throwable e) which returns the specific fault if it can be reconstructed
    // from the exception or null. Then instead of having a case per exception, we can have a case per fault, which
    // should be cool because there is a 1:1 mapping between faults and exceptions (apart from the more geeneric
    // UnknownFaults and MSQExceptions)
    while (cause != null) {

      if (cause instanceof MSQException) {
        return ((MSQException) cause).getFault();
      } else if (cause instanceof ParseException) {
        return new CannotParseExternalDataFault(cause.getMessage());
      } else if (cause instanceof UnsupportedColumnTypeException) {
        final UnsupportedColumnTypeException unsupportedColumnTypeException = (UnsupportedColumnTypeException) cause;
        return new ColumnTypeNotSupportedFault(
            unsupportedColumnTypeException.getColumnName(),
            unsupportedColumnTypeException.getColumnType()
        );
      } else if (cause instanceof TooManyBucketsException) {
        return new TooManyBucketsFault(((TooManyBucketsException) cause).getMaxBuckets());
      } else if (cause instanceof FrameRowTooLargeException) {
        return new RowTooLargeFault(((FrameRowTooLargeException) cause).getMaxFrameSize());
      } else if (cause instanceof InvalidNullByteException) {
        InvalidNullByteException invalidNullByteException = (InvalidNullByteException) cause;
        String columnName = invalidNullByteException.getColumn();
        if (columnMappings != null) {
          IntList outputColumnsForQueryColumn = columnMappings.getOutputColumnsForQueryColumn(columnName);

          // outputColumnsForQueryColumn.size should always be 1 due to hasUniqueOutputColumnNames check that is done
          if (outputColumnsForQueryColumn.size() >= 1) {
            int outputColumn = outputColumnsForQueryColumn.getInt(0);
            columnName = columnMappings.getOutputColumnName(outputColumn);
          }
        }

        return new InvalidNullByteFault(
            invalidNullByteException.getSource(),
            invalidNullByteException.getRowNumber(),
            columnName,
            invalidNullByteException.getValue(),
            invalidNullByteException.getPosition()
        );
      } else if (cause instanceof UnexpectedMultiValueDimensionException) {
        return new QueryRuntimeFault(StringUtils.format(
            "Column [%s] is a multi-value string. Please wrap the column using MV_TO_ARRAY() to proceed further.",
            ((UnexpectedMultiValueDimensionException) cause).getDimensionName()
        ), cause.getMessage());
      } else if (cause.getClass().getPackage().getName().startsWith("org.apache.druid.query")) {
        // catch all for all query runtime exception faults.
        return new QueryRuntimeFault(e.getMessage(), null);
      } else {
        cause = cause.getCause();
      }
    }

    return UnknownFault.forException(e);
  }
}
