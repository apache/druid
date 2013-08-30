/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
