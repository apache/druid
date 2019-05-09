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

package org.apache.druid.java.util.common.logger;

import org.apache.druid.java.util.common.StringUtils;
import org.slf4j.LoggerFactory;

/**
 */
public class Logger
{
  private final org.slf4j.Logger log;

  public Logger(String name)
  {
    log = LoggerFactory.getLogger(name);
  }

  public Logger(Class clazz)
  {
    log = LoggerFactory.getLogger(clazz);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("Logger{name=[%s], class[%s]}", log.getName(), log.getClass());
  }

  public void trace(String message, Object... formatArgs)
  {
    if (log.isTraceEnabled()) {
      log.trace(StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void debug(String message, Object... formatArgs)
  {
    if (log.isDebugEnabled()) {
      log.debug(StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void debug(Throwable t, String message, Object... formatArgs)
  {
    if (log.isDebugEnabled()) {
      log.debug(StringUtils.nonStrictFormat(message, formatArgs), t);
    }
  }

  public void info(String message, Object... formatArgs)
  {
    if (log.isInfoEnabled()) {
      log.info(StringUtils.nonStrictFormat(message, formatArgs));
    }
  }

  public void info(Throwable t, String message, Object... formatArgs)
  {
    if (log.isInfoEnabled()) {
      log.info(StringUtils.nonStrictFormat(message, formatArgs), t);
    }
  }

  /**
   * Protect against assuming slf4j convention. use `warn(Throwable t, String message, Object... formatArgs)` instead
   *
   * @param message The string message
   * @param t       The Throwable to log
   */
  @Deprecated
  public void warn(String message, Throwable t)
  {
    log.warn(message, t);
  }

  public void warn(String message, Object... formatArgs)
  {
    log.warn(StringUtils.nonStrictFormat(message, formatArgs));
  }

  public void warn(Throwable t, String message, Object... formatArgs)
  {
    log.warn(StringUtils.nonStrictFormat(message, formatArgs), t);
  }

  public void error(String message, Object... formatArgs)
  {
    log.error(StringUtils.nonStrictFormat(message, formatArgs));
  }

  /**
   * Protect against assuming slf4j convention. use `error(Throwable t, String message, Object... formatArgs)` instead
   *
   * @param message The string message
   * @param t       The Throwable to log
   */
  @Deprecated
  public void error(String message, Throwable t)
  {
    log.error(message, t);
  }

  public void error(Throwable t, String message, Object... formatArgs)
  {
    log.error(StringUtils.nonStrictFormat(message, formatArgs), t);
  }

  public void assertionError(String message, Object... formatArgs)
  {
    log.error("ASSERTION_ERROR: " + message, formatArgs);
  }

  public void wtf(String message, Object... formatArgs)
  {
    log.error(StringUtils.nonStrictFormat("WTF?!: " + message, formatArgs), new Exception());
  }

  public void wtf(Throwable t, String message, Object... formatArgs)
  {
    log.error(StringUtils.nonStrictFormat("WTF?!: " + message, formatArgs), t);
  }

  public boolean isTraceEnabled()
  {
    return log.isTraceEnabled();
  }

  public boolean isDebugEnabled()
  {
    return log.isDebugEnabled();
  }

  public boolean isInfoEnabled()
  {
    return log.isInfoEnabled();
  }
}
