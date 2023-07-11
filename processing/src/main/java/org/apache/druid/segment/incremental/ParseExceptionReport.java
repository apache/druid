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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

public class ParseExceptionReport
{
  private final String input;
  private final String errorType;
  private final List<String> details;
  private final long timeOfExceptionMillis;

  @JsonCreator
  public ParseExceptionReport(
      @JsonProperty("input") String input,
      @JsonProperty("errorType") String errorType,
      @JsonProperty("details") List<String> details,
      @JsonProperty("timeOfExceptionMillis") long timeOfExceptionMillis
  )
  {
    this.input = input;
    this.errorType = errorType;
    this.details = details;
    this.timeOfExceptionMillis = timeOfExceptionMillis;
  }

  @JsonProperty
  public String getInput()
  {
    return input;
  }

  @JsonProperty
  public String getErrorType()
  {
    return errorType;
  }

  @JsonProperty
  public List<String> getDetails()
  {
    return details;
  }

  @JsonProperty
  public long getTimeOfExceptionMillis()
  {
    return timeOfExceptionMillis;
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
    ParseExceptionReport that = (ParseExceptionReport) o;
    return timeOfExceptionMillis == that.timeOfExceptionMillis
           && input.equals(that.input)
           && errorType.equals(that.errorType)
           && details.equals(that.details);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(input, errorType, details, timeOfExceptionMillis);
  }

  @Override
  public String toString()
  {
    return "ParseExceptionReport{" +
           "input='" + input + '\'' +
           ", errorType='" + errorType + '\'' +
           ", details=" + details +
           ", timeOfExceptionMillis=" + timeOfExceptionMillis +
           '}';
  }
}
