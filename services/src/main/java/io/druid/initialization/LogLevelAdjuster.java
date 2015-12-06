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

package io.druid.initialization;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 */
public class LogLevelAdjuster implements LogLevelAdjusterMBean
{
  private static final Logger log = Logger.getLogger(LogLevelAdjuster.class);

  private static volatile boolean registered = false;
  public synchronized static void register() throws Exception
  {
    if (! registered) {
      ManagementFactory.getPlatformMBeanServer().registerMBean(
          new LogLevelAdjuster(),
          new ObjectName("log4j:name=log4j")
      );
      registered = true;
    }
  }

  @Override
  public String getLevel(String packageName)
  {
    final Level level = Logger.getLogger(packageName).getEffectiveLevel();

    if (log.isInfoEnabled()) {
      log.info(String.format("Asked to look up level for package[%s] => [%s]", packageName, level));
    }

    return level.toString();
  }

  @Override
  public void setLevel(String packageName, String level)
  {
    final Level theLevel = Level.toLevel(level, null);
    if (theLevel == null) {
      throw new IllegalArgumentException("Unknown level: " + level);
    }

    if (log.isInfoEnabled()) {
      log.info(String.format("Setting log level for package[%s] => [%s]", packageName, theLevel));
    }

    Logger.getLogger(packageName).setLevel(theLevel);
  }
}
