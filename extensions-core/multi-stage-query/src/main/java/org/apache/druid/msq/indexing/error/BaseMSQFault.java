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
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Base implementation of {@link MSQFault}.
 *
 * Implements {@link #equals}, {@link #hashCode()}, and {@link #toString()} using {@link #errorCode} and
 * {@link #errorMessage}, so faults must either encode all relevant information in the message, or provide
 * their own implementation of these methods.
 */
public abstract class BaseMSQFault implements MSQFault
{
  private final String errorCode;

  @Nullable
  private final String errorMessage;

  BaseMSQFault(final String errorCode, @Nullable final String errorMessage)
  {
    this.errorCode = Preconditions.checkNotNull(errorCode, "errorCode");
    this.errorMessage = errorMessage;
  }

  BaseMSQFault(
      final String errorCode,
      final String errorMessageFormat,
      final Object errorMessageFirstArg,
      final Object... errorMessageOtherArgs
  )
  {
    this(errorCode, format(errorMessageFormat, errorMessageFirstArg, errorMessageOtherArgs));
  }

  BaseMSQFault(final String errorCode)
  {
    this(errorCode, null);
  }

  @Override
  public String getErrorCode()
  {
    return errorCode;
  }

  @Override
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getErrorMessage()
  {
    return errorMessage;
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
    BaseMSQFault that = (BaseMSQFault) o;
    return Objects.equals(errorCode, that.errorCode) && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(errorCode, errorMessage);
  }

  @Override
  public String toString()
  {
    return getCodeWithMessage();
  }

  private static String format(
      final String formatString,
      final Object firstArg,
      final Object... otherArgs
  )
  {
    final Object[] args = new Object[1 + (otherArgs != null ? otherArgs.length : 0)];

    args[0] = firstArg;

    if (otherArgs != null) {
      System.arraycopy(otherArgs, 0, args, 1, otherArgs.length);
    }

    return StringUtils.format(formatString, args);
  }
}
