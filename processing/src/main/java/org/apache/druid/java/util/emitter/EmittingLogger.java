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
import java.util.Map;

/**
 * {@link Logger} which also has an {@link ServiceEmitter}. Primarily useful for constructing and emitting "alerts" in
 * the form of {@link AlertBuilder}, which will log when {@link AlertBuilder#emit()} is called.
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

  /**
   * Make an {@link AlertBuilder} which will call {@link #warn(String, Object...)} when {@link AlertBuilder#emit()} is
   * called.
   */
  public AlertBuilder makeWarningAlert(String message, Object... objects)
  {
    return makeAlert(null, false, message, objects);
  }

  /**
   * Make an {@link AlertBuilder} which will call {@link #error(String, Object...)} when {@link AlertBuilder#emit()} is
   * called.
   */
  public AlertBuilder makeAlert(String message, Object... objects)
  {
    return makeAlert(null, true, message, objects);
  }

  /**
   * Make an {@link AlertBuilder} which will call {@link #error(Throwable, String, Object...)} when
   * {@link AlertBuilder#emit()} is called.
   */
  public AlertBuilder makeAlert(@Nullable Throwable t, String message, Object... objects)
  {
    return makeAlert(t, true, message, objects);
  }

  public AlertBuilder makeAlert(@Nullable Throwable t, boolean isError, String message, Object... objects)
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

    return new LoggingAlertBuilder(t, StringUtils.format(message, objects), emitter, isError)
        .addData("class", className);
  }

  public class LoggingAlertBuilder extends AlertBuilder
  {
    private final Throwable t;
    private final boolean isError;

    private volatile boolean emitted = false;

    private LoggingAlertBuilder(Throwable t, String description, ServiceEmitter emitter, boolean isError)
    {
      super(description, emitter);
      this.t = t;
      this.isError = isError;
      addThrowable(t);
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
        if (isError) {
          error(format, description, dataMap);
        } else {
          warn(format, description, dataMap);
        }
      } else {
        // Filter out the stack trace from the message, because it should be in the logline already if it's wanted.
        final Map<String, Object> filteredDataMap = Maps.filterKeys(
            dataMap,
            Predicates.not(Predicates.equalTo("exceptionStackTrace"))
        );
        if (isError) {
          error(t, format, description, filteredDataMap);
        } else {
          warn(t, format, description, filteredDataMap);
        }
      }
    }
  }
}
