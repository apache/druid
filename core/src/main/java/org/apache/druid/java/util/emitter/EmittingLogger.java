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

package org.apache.druid.java.util.emitter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.StringWriter;

/**
 */
public class EmittingLogger extends Logger
{
  public static final String EXCEPTION_TYPE_KEY = "exceptionType";
  public static final String EXCEPTION_MESSAGE_KEY = "exceptionMessage";
  public static final String EXCEPTION_STACK_TRACE_KEY = "exceptionStackTrace";

  private static volatile ServiceEmitter emitter = null;

  private final String className;

  public static void registerEmitter(ServiceEmitter emitter)
  {
    Preconditions.checkNotNull(emitter);
    EmittingLogger.emitter = emitter;
  }

  public EmittingLogger(Class clazz)
  {
    super(clazz);
    this.className = clazz.getName();
  }

  private EmittingLogger(org.slf4j.Logger log, boolean stackTraces)
  {
    super(log, stackTraces);
    this.className = log.getName();
  }

  @Override
  public EmittingLogger noStackTrace()
  {
    return new EmittingLogger(getSlf4jLogger(), false);
  }

  public AlertBuilder makeAlert(String message, Object... objects)
  {
    return makeAlert(null, message, objects);
  }

  public AlertBuilder makeAlert(@Nullable Throwable t, String message, Object... objects)
  {
    if (emitter == null) {
      final String errorMessage = StringUtils.format(
          "Emitter not initialized!  Cannot alert.  Please make sure to call %s.registerEmitter()\n"
          + "Message: %s",
          this.getClass(),
          StringUtils.nonStrictFormat(message, objects)
      );

      ISE e = new ISE(errorMessage);
      if (t != null) {
        e.addSuppressed(t);
      }

      error(e, errorMessage);

      throw e;
    }

    final AlertBuilder retVal = new EmittingAlertBuilder(t, StringUtils.format(message, objects), emitter)
        .addData("class", className);

    if (t != null) {
      final StringWriter trace = new StringWriter();
      final PrintWriter pw = new PrintWriter(trace);
      t.printStackTrace(pw);
      retVal.addData("exceptionType", t.getClass());
      retVal.addData("exceptionMessage", t.getMessage());
      retVal.addData("exceptionStackTrace", trace.toString());
    }

    return retVal;
  }

  public class EmittingAlertBuilder extends AlertBuilder
  {
    private final Throwable t;

    private volatile boolean emitted = false;

    private EmittingAlertBuilder(Throwable t, String description, ServiceEmitter emitter)
    {
      super(description, emitter);
      this.t = t;
    }

    @Override
    public void emit()
    {
      logIt("%s: %s");

      emitted = true;

      super.emit();
    }

    @Override
    protected void finalize()
    {
      if (!emitted) {
        logIt("Alert not emitted, emitting. %s: %s");
        super.emit();
      }
    }

    private void logIt(String format)
    {
      if (t == null) {
        error(format, description, dataMap);
      } else {
        // Filter out the stack trace from the message, because it should be in the logline already if it's wanted.
        error(
            t,
            format,
            description,
            Maps.filterKeys(dataMap, Predicates.not(Predicates.equalTo("exceptionStackTrace")))
        );
      }
    }
  }
}
