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

package org.apache.druid.server.coordination.startup;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;

import java.util.Locale;

/**
 * Eagerly loads column metadata for all segments at Historical startup.
 * <p>
 * Optimizes for predictable first-query latency at the cost of longer startup time and higher I/O during bootstrap.
 * {@link #shouldLoadLazily(DataSegment)} always returns {@code false}.
 */
public class LoadAllEagerlyStrategy implements HistoricalStartupCacheLoadStrategy
{
  private static final Logger log = new Logger(LoadAllEagerlyStrategy.class);

  public static final String STRATEGY_NAME = "loadAllEagerly";

  public LoadAllEagerlyStrategy()
  {
    log.info("Using [%s] strategy", STRATEGY_NAME);
  }

  @Override
  public boolean shouldLoadLazily(DataSegment segment)
  {
    return false;
  }

  @Override
  public String toString()
  {
    return String.format(Locale.ROOT, "{type=%s}", STRATEGY_NAME);
  }
}
