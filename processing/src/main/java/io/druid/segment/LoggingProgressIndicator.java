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

package io.druid.segment;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

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
    Stopwatch sectionWatch = sections.get(section);
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
