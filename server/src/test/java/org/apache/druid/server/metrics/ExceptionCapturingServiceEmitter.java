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

package org.apache.druid.server.metrics;

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import javax.annotation.Nullable;
import java.util.Map;

public class ExceptionCapturingServiceEmitter extends ServiceEmitter
{
  private volatile Class exceptionClass;
  private volatile String exceptionMessage;
  private volatile String stackTrace;

  public ExceptionCapturingServiceEmitter()
  {
    super("", "", null);
  }

  @Override
  public void emit(Event event)
  {
    //noinspection unchecked
    final Map<String, Object> dataMap = (Map<String, Object>) event.toMap().get("data");
    final Class exceptionClass = (Class) dataMap.get(EmittingLogger.EXCEPTION_TYPE_KEY);
    if (exceptionClass != null) {
      final String exceptionMessage = (String) dataMap.get(EmittingLogger.EXCEPTION_MESSAGE_KEY);
      final String stackTrace = (String) dataMap.get(EmittingLogger.EXCEPTION_STACK_TRACE_KEY);
      this.exceptionClass = exceptionClass;
      this.exceptionMessage = exceptionMessage;
      this.stackTrace = stackTrace;
    }
  }

  @Nullable
  public Class getExceptionClass()
  {
    return exceptionClass;
  }

  @Nullable
  public String getExceptionMessage()
  {
    return exceptionMessage;
  }

  @Nullable
  public String getStackTrace()
  {
    return stackTrace;
  }
}
