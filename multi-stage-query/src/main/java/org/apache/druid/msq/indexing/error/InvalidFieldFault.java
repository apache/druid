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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;
import java.util.Objects;

@JsonTypeName(InvalidFieldFault.CODE)
public class InvalidFieldFault extends BaseMSQFault
{
  static final String CODE = "InvalidField";

  @Nullable
  private final String source;
  @Nullable
  private final String column;
  @Nullable
  private final Integer rowNumber;
  @Nullable
  private final String errorMsg;
  @Nullable
  private final String logMsg;

  public InvalidFieldFault(
      @Nullable @JsonProperty("source") String source,
      @Nullable @JsonProperty("column") String column,
      @Nullable @JsonProperty("rowNumber") Integer rowNumber,
      @Nullable @JsonProperty("errorMsg") String errorMsg,
      @Nullable @JsonProperty("logMsg") String logMsg
  )
  {
    super(
        CODE,
        logMsg
    );
    this.column = column;
    this.rowNumber = rowNumber;
    this.source = source;
    this.errorMsg = errorMsg;
    this.logMsg = logMsg;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getColumn()
  {
    return column;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getRowNumber()
  {
    return rowNumber;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSource()
  {
    return source;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMsg()
  {
    return errorMsg;
  }
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getLogMsg()
  {
    return logMsg;
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
    if (!super.equals(o)) {
      return false;
    }
    InvalidFieldFault that = (InvalidFieldFault) o;
    return Objects.equals(source, that.source)
           && Objects.equals(column, that.column)
           && Objects.equals(rowNumber, that.rowNumber)
           && Objects.equals(errorMsg, that.errorMsg)
           && Objects.equals(logMsg, that.logMsg);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), source, column, rowNumber, errorMsg, logMsg);
  }
}
