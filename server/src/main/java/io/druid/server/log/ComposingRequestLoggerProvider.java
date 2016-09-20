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

package io.druid.server.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.server.RequestLogLine;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;

/**
 */
@JsonTypeName("composing")
public class ComposingRequestLoggerProvider implements RequestLoggerProvider
{
  @JsonProperty
  @NotNull
  private List<RequestLoggerProvider> requestLoggers = Lists.newArrayList();

  @Override
  public RequestLogger get()
  {
    final List<RequestLogger> loggers = Lists.newArrayList(Lists.transform(
        requestLoggers,
        new Function<RequestLoggerProvider, RequestLogger>()
        {
          @Override
          public RequestLogger apply(RequestLoggerProvider input)
          {
            return input.get();
          }
        }
    ));
    return new ComposingRequestLogger(loggers);
  }

  public static class ComposingRequestLogger implements RequestLogger
  {

    private final List<RequestLogger> loggers;

    public ComposingRequestLogger(List<RequestLogger> loggers)
    {
      this.loggers = loggers;
    }

    @Override
    public void log(RequestLogLine requestLogLine) throws IOException
    {
      IOException exception = null;
      for (RequestLogger logger : loggers) {
        try {
          logger.log(requestLogLine);
        }
        catch (IOException e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
      }
      if (exception != null) {
        throw exception;
      }
    }
  }

}
