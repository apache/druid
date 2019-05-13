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

package org.apache.druid.server.log;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.RequestLogLine;

/**
 * This {@link RequestLogEventBuilderFactory} creates builders that return {@link DefaultRequestLogEvent}s.
 */
public final class DefaultRequestLogEventBuilderFactory implements RequestLogEventBuilderFactory
{
  private static final DefaultRequestLogEventBuilderFactory INSTANCE = new DefaultRequestLogEventBuilderFactory();

  @JsonCreator
  public static DefaultRequestLogEventBuilderFactory instance()
  {
    return INSTANCE;
  }

  private DefaultRequestLogEventBuilderFactory()
  {
  }

  @Override
  public ServiceEventBuilder<RequestLogEvent> createRequestLogEventBuilder(String feed, RequestLogLine requestLogLine)
  {
    return new ServiceEventBuilder<RequestLogEvent>()
    {
      @Override
      public RequestLogEvent build(ImmutableMap<String, String> serviceDimensions)
      {
        return new DefaultRequestLogEvent(serviceDimensions, feed, requestLogLine);
      }
    };
  }

  @Override
  public String toString()
  {
    return getClass().getName();
  }
}
