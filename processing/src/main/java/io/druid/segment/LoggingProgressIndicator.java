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

package io.druid.segment;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 */
public class LoggingProgressIndicator extends BaseProgressIndicator
{
  private static Logger log = new Logger(LoggingProgressIndicator.class);

  private final String progressName;
  private final Stopwatch global;

  private final Map<String, Stopwatch> sections = Maps.newHashMap();

  public LoggingProgressIndicator(String progressName)
  {
    this.progressName = progressName;
    this.global = Stopwatch.createUnstarted();
  }

  @Override
  public void start()
  {
    log.info("Starting [%s]", progressName);
    global.start();
  }

  @Override
  public void stop()
  {
    long time = global.elapsed(TimeUnit.MILLISECONDS);
    global.stop();

    log.info("[%s] complete. Elapsed time: [%,d] millis", progressName, time);
  }

  @Override
  public void startSection(String section)
  {
    log.info("[%s]: Starting [%s]", progressName, section);

    Stopwatch sectionWatch = sections.get(section);
    if (sectionWatch != null) {
      throw new ISE("[%s]: Cannot start progress tracker for [%s]. It is already started.", progressName, section);
    }
    sectionWatch = Stopwatch.createStarted();
    sections.put(section, sectionWatch);
  }

  @Override
  public void progressSection(String section, String message)
  {
    Stopwatch sectionWatch = sections.remove(section);
    if (sectionWatch == null) {
      throw new ISE("[%s]: Cannot progress tracker for [%s]. Nothing started.", progressName, section);
    }
    long time = sectionWatch.elapsed(TimeUnit.MILLISECONDS);
    log.info("[%s]: [%s] : %s. Elapsed time: [%,d] millis", progressName, section, message, time);
  }

  @Override
  public void stopSection(String section)
  {
    Stopwatch sectionWatch = sections.remove(section);
    if (sectionWatch == null) {
      throw new ISE("[%s]: Cannot stop progress tracker for [%s]. Nothing started.", progressName, section);
    }
    long time = sectionWatch.elapsed(TimeUnit.MILLISECONDS);
    sectionWatch.stop();

    log.info("[%s]: [%s] has completed. Elapsed time: [%,d] millis", progressName, section, time);
  }
}
