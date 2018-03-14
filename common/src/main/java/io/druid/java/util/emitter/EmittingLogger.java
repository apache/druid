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

package io.druid.java.util.emitter;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.AlertBuilder;
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 */
public class EmittingLogger extends Logger
{
  private static volatile ServiceEmitter emitter = null;

  private final String className;

  public static void registerEmitter(ServiceEmitter emitter)
  {
    Preconditions.checkNotNull(emitter);
    EmittingLogger.emitter = emitter;
  }

  public EmittingLogger(String className)
  {
    super(className);

    this.className = className;
  }

  public EmittingLogger(Class clazz)
  {
    super(clazz);

    this.className = clazz.getName();
  }

  public AlertBuilder makeAlert(String message, Object... objects)
  {
    return makeAlert(null, message, objects);
  }

  public AlertBuilder makeAlert(Throwable t, String message, Object... objects)
  {
    if (emitter == null) {
      final String errorMessage = StringUtils.format(
          "Emitter not initialized!  Cannot alert.  Please make sure to call %s.registerEmitter()", this.getClass()
      );

      error(errorMessage);
      throw new ISE(errorMessage);
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
    protected void finalize() throws Throwable
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
