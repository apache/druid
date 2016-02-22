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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

public class QueryInterruptedException extends RuntimeException
{
  public static final String QUERY_INTERRUPTED = "Query interrupted";
  public static final String QUERY_TIMEOUT = "Query timeout";
  public static final String QUERY_CANCELLED = "Query cancelled";
  public static final String UNKNOWN_EXCEPTION = "Unknown exception";

  private static final Set<String> listKnownException = ImmutableSet.of(
      QUERY_CANCELLED,
      QUERY_INTERRUPTED,
      QUERY_TIMEOUT,
      UNKNOWN_EXCEPTION
  );

  @JsonProperty
  private final String causeMessage;
  @JsonProperty
  private final String host;

  @JsonCreator
  public QueryInterruptedException(
      @JsonProperty("error") String message,
      @JsonProperty("causeMessage") String causeMessage,
      @JsonProperty("host") String host
  )
  {
    super(message);
    this.causeMessage = causeMessage;
    this.host = host;
  }

  public QueryInterruptedException(Throwable cause)
  {
    this(cause, null);
  }

  public QueryInterruptedException(Throwable e, String host)
  {
    super(e);
    this.host = host;
    causeMessage = e.getMessage();
  }

  @JsonProperty("error")
  @Override
  public String getMessage()
  {
    if (this.getCause() == null) {
      return super.getMessage();
    } else if (this.getCause() instanceof QueryInterruptedException) {
      return getCause().getMessage();
    } else if (this.getCause() instanceof InterruptedException) {
      return QUERY_INTERRUPTED;
    } else if (this.getCause() instanceof CancellationException) {
      return QUERY_CANCELLED;
    } else if (this.getCause() instanceof TimeoutException) {
      return QUERY_TIMEOUT;
    } else {
      return UNKNOWN_EXCEPTION;
    }
  }

  @JsonProperty("causeMessage")
  public String getCauseMessage()
  {
    return causeMessage;
  }

  @JsonProperty("host")
  public String getHost()
  {
    return host;
  }

  public boolean isNotKnown()
  {
    return !listKnownException.contains(getMessage());
  }
}
