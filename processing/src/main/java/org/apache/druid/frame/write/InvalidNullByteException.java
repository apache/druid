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

import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * Exception thrown by {@link FrameWriterUtils#copyByteBufferToMemory} if configured to check for null bytes
 * and a null byte is encountered.
 */
public class InvalidNullByteException extends RuntimeException
{

  @Nullable
  private final String source;

  @Nullable
  private final Integer rowNumber;

  @Nullable
  private final String column;

  @Nullable
  private final String value;

  @Nullable
  private final Integer position;

  private InvalidNullByteException(
      @Nullable final String source,
      @Nullable final Integer rowNumber,
      @Nullable final String column,
      @Nullable final String value,
      @Nullable final Integer position
  )
  {
    super(StringUtils.format(
        "Encountered null byte at source[%s], rowNumber[%d], column[%s], value[%s], position[%s]",
        source,
        rowNumber,
        column,
        value,
        position
    ));
    this.source = source;
    this.rowNumber = rowNumber;
    this.column = column;
    this.value = value;
    this.position = position;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static Builder builder(InvalidNullByteException invalidNullByteException)
  {
    return new Builder(invalidNullByteException);
  }


  @Nullable
  public Integer getRowNumber()
  {
    return rowNumber;
  }

  @Nullable
  public String getColumn()
  {
    return column;
  }

  @Nullable
  public String getValue()
  {
    return value;
  }

  @Nullable
  public String getSource()
  {
    return source;
  }

  @Nullable
  public Integer getPosition()
  {
    return position;
  }

  public static class Builder
  {
    @Nullable
    private String source;

    @Nullable
    private Integer rowNumber;

    @Nullable
    private String column;

    @Nullable
    private String value;

    @Nullable
    private Integer position;

    public Builder()
    {

    }

    public Builder(InvalidNullByteException invalidNullByteException)
    {
      this.source = invalidNullByteException.source;
      this.rowNumber = invalidNullByteException.rowNumber;
      this.column = invalidNullByteException.column;
      this.value = invalidNullByteException.value;
      this.position = invalidNullByteException.position;
    }

    public InvalidNullByteException build()
    {
      return new InvalidNullByteException(source, rowNumber, column, value, position);
    }

    public Builder source(final String source)
    {
      this.source = source;
      return this;
    }

    public Builder rowNumber(final Integer rowNumber)
    {
      this.rowNumber = rowNumber;
      return this;
    }

    public Builder column(final String column)
    {
      this.column = column;
      return this;
    }

    public Builder value(final String value)
    {
      this.value = value;
      return this;
    }

    public Builder position(final Integer position)
    {
      this.position = position;
      return this;
    }
  }
}
