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

package org.apache.druid.frame.write;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

public class FrameFieldWriterException extends RuntimeException
{
  @Nullable
  private final String source;
  @Nullable
  private final String column;
  @Nullable
  private final Integer rowNumber;
  @Nullable
  private final String errorMsg;

  private FrameFieldWriterException(
      @Nullable @JsonProperty("source") String source,
      @Nullable @JsonProperty("column") String column,
      @Nullable @JsonProperty("rowNumber") Integer rowNumber,
      @Nullable @JsonProperty("message") String errorMsg
  )
  {
    super(StringUtils.format(
        "Error[%s] while writing a field for source[%s], rowNumber[%d], column[%s].",
        errorMsg,
        source,
        rowNumber,
        column
    ));
    this.column = column;
    this.rowNumber = rowNumber;
    this.source = source;
    this.errorMsg = errorMsg;
  }

  @Nullable
  public String getColumn()
  {
    return column;
  }

  @Nullable
  public Integer getRowNumber()
  {
    return rowNumber;
  }

  @Nullable
  public String getSource()
  {
    return source;
  }

  @Nullable
  public String getErrorMsg()
  {
    return errorMsg;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(FrameFieldWriterException frameFieldWriterException)
  {
    return new Builder(frameFieldWriterException);
  }

  public static class Builder
  {
    @Nullable
    private String column;
    @Nullable
    private Integer rowNumber;
    @Nullable
    private String source;
    @Nullable
    private String errorMsg;

    public Builder()
    {
    }

    public Builder(FrameFieldWriterException frameFieldWriterException)
    {
      source = frameFieldWriterException.source;
      rowNumber = frameFieldWriterException.rowNumber;
      column = frameFieldWriterException.column;
      errorMsg = frameFieldWriterException.errorMsg;
    }

    public FrameFieldWriterException build()
    {
      return new FrameFieldWriterException(source, column, rowNumber, errorMsg);
    }

    public Builder column(String val)
    {
      column = val;
      return this;
    }

    public Builder rowNumber(Integer val)
    {
      rowNumber = val;
      return this;
    }

    public Builder source(String val)
    {
      source = val;
      return this;
    }

    public Builder errorMsg(String val)
    {
      errorMsg = val;
      return this;
    }
  }
}
