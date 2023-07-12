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

package org.apache.druid.server.coordinator.stats;

/**
 * A coordinator statistic, which may or may not be emitted as a metric.
 */
public class CoordinatorStat
{
  private final String metricName;
  private final String shortName;
  private final Level level;

  /**
   * Creates a new DEBUG level stat which is not emitted as a metric.
   *
   * @param shortName Unique name used while logging the stat
   */
  public static CoordinatorStat toDebugOnly(String shortName)
  {
    return new CoordinatorStat(shortName, null, Level.DEBUG);
  }

  /**
   * Creates a new DEBUG level stat which is also emitted as a metric.
   *
   * @param shortName  Unique name used while logging the stat
   * @param metricName Name to be used when emitting this stat as a metric
   */
  public static CoordinatorStat toDebugAndEmit(String shortName, String metricName)
  {
    return new CoordinatorStat(shortName, metricName, Level.DEBUG);
  }

  /**
   * Creates a new stat of the specified level, which is also emitted as a metric.
   *
   * @param shortName  Unique name used while logging the stat
   * @param metricName Name to be used when emitting this stat as a metric
   * @param level      Logging level for this stat
   */
  public static CoordinatorStat toLogAndEmit(String shortName, String metricName, Level level)
  {
    return new CoordinatorStat(shortName, metricName, level);
  }

  private CoordinatorStat(String shortStatName, String metricName, Level level)
  {
    this.metricName = metricName;
    this.shortName = shortStatName;
    this.level = level == null ? Level.DEBUG : level;
  }

  /**
   * @return Metric name to be used when emitting this stat. {@code null} if
   * this stat should not be emitted.
   */
  public String getMetricName()
  {
    return metricName;
  }

  /**
   * Unique name used while logging this stat.
   */
  public String getShortName()
  {
    return shortName;
  }

  /**
   * Level of this stat, typically used for logging.
   */
  public Level getLevel()
  {
    return level;
  }

  /**
   * Whether this statistic should be emitted as a metric.
   */
  public boolean shouldEmit()
  {
    return metricName != null;
  }

  @Override
  public String toString()
  {
    return shortName;
  }

  /**
   * Level of coordinator stat, typically used for logging.
   */
  public enum Level
  {
    DEBUG, INFO, ERROR
  }

}
