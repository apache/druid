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

package org.apache.druid.testing.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;

public class WikipediaStreamEventStreamGenerator extends SyntheticStreamGenerator
{
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

  public WikipediaStreamEventStreamGenerator(int eventsPerSeconds, long cyclePaddingMs)
  {
    super(eventsPerSeconds, cyclePaddingMs);
  }

  @Override
  Object getEvent(int i, DateTime timestamp)
  {
    Map<String, Object> event = new HashMap<>();
    event.put("page", "Gypsy Danger");
    event.put("language", "en");
    event.put("user", "nuclear");
    event.put("unpatrolled", "true");
    event.put("newPage", "true");
    event.put("robot", "false");
    event.put("anonymous", "false");
    event.put("namespace", "article");
    event.put("continent", "North America");
    event.put("country", "United States");
    event.put("region", "Bay Area");
    event.put("city", "San Francisco");
    event.put("timestamp", DATE_TIME_FORMATTER.print(timestamp));
    event.put("added", i);
    event.put("deleted", 0);
    event.put("delta", i);
    return event;
  }
}
