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

import org.apache.druid.java.util.common.Pair;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WikipediaStreamEventStreamGenerator extends SyntheticStreamGenerator
{
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

  public WikipediaStreamEventStreamGenerator(EventSerializer serializer, int eventsPerSeconds, long cyclePaddingMs)
  {
    super(serializer, eventsPerSeconds, cyclePaddingMs);
  }

  @Override
  List<Pair<String, Object>> newEvent(int i, DateTime timestamp)
  {
    List<Pair<String, Object>> event = new ArrayList<>();
    event.add(Pair.of("timestamp", DATE_TIME_FORMATTER.print(timestamp)));
    event.add(Pair.of("page", "Gypsy Danger"));
    event.add(Pair.of("language", "en"));
    event.add(Pair.of("user", "nuclear"));
    event.add(Pair.of("unpatrolled", "true"));
    event.add(Pair.of("newPage", "true"));
    event.add(Pair.of("robot", "false"));
    event.add(Pair.of("anonymous", "false"));
    event.add(Pair.of("namespace", "article"));
    event.add(Pair.of("continent", "North America"));
    event.add(Pair.of("country", "United States"));
    event.add(Pair.of("region", "Bay Area"));
    event.add(Pair.of("city", "San Francisco"));
    event.add(Pair.of("added", i));
    event.add(Pair.of("deleted", 0));
    event.add(Pair.of("delta", i));
    return Collections.unmodifiableList(event);
  }
}
