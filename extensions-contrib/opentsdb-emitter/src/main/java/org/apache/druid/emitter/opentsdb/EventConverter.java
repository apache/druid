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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class EventConverter
{
  private static final Logger log = new Logger(EventConverter.class);
  private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");

  private final Map<String, Set<String>> metricMap;

  public EventConverter(ObjectMapper mapper, String metricMapPath)
  {
    metricMap = readMap(mapper, metricMapPath);
  }

  protected String sanitize(String metric)
  {
    return WHITESPACE.matcher(metric.trim()).replaceAll("_").replace('/', '.');
  }

  /**
   * This function will convert a druid event to a opentsdb event.
   * Also this function acts as a filter. It returns <tt>null</tt> if the event is not suppose to be emitted to Opentsdb.
   * And it will filter out dimensions which is not suppose to be emitted.
   *
   * @param serviceMetricEvent Druid event ot type {@link ServiceMetricEvent}
   *
   * @return {@link OpentsdbEvent} or <tt>null</tt>
   */
  public OpentsdbEvent convert(ServiceMetricEvent serviceMetricEvent)
  {
    String metric = serviceMetricEvent.getMetric();
    if (!metricMap.containsKey(metric)) {
      return null;
    }

    long timestamp = serviceMetricEvent.getCreatedTime().getMillis() / 1000L;
    Number value = serviceMetricEvent.getValue();

    Map<String, Object> tags = new HashMap<>();
    String service = serviceMetricEvent.getService().replace(':', '_');
    String host = serviceMetricEvent.getHost().replace(':', '_');
    tags.put("service", service);
    tags.put("host", host);

    Map<String, Object> userDims = serviceMetricEvent.getUserDims();
    for (String dim : metricMap.get(metric)) {
      if (userDims.containsKey(dim)) {
        Object dimValue = userDims.get(dim);
        if (dimValue instanceof String) {
          dimValue = ((String) dimValue).replace(':', '_');
        }
        tags.put(dim, dimValue);
      }
    }

    return new OpentsdbEvent(sanitize(metric), timestamp, value, tags);
  }

  private Map<String, Set<String>> readMap(ObjectMapper mapper, String metricMapPath)
  {
    try {
      InputStream is;
      if (Strings.isNullOrEmpty(metricMapPath)) {
        log.info("Using default metric map");
        is = this.getClass().getClassLoader().getResourceAsStream("defaultMetrics.json");
      } else {
        log.info("Using default metric map located at [%s]", metricMapPath);
        is = new FileInputStream(new File(metricMapPath));
      }
      return mapper.readerFor(new TypeReference<Map<String, Set<String>>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metrics and dimensions");
    }
  }
}
