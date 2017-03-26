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

package io.druid.query;

import org.joda.time.Interval;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DataSourceUtil
{
  public static String getMetricName(DataSource dataSource)
  {
    final List<String> names = dataSource.getNames();
    return names.size() == 1 ? names.get(0) : names.toString();
  }

  public static String getMetricName(Iterable<DataSourceWithSegmentSpec> dataSources)
  {
    return StreamSupport.stream(dataSources.spliterator(), false)
                 .map(DataSourceUtil::getMetricName)
                 .collect(Collectors.joining(",", "[", "]"));
  }

  private static final StringJoiner JOINER = new StringJoiner(",", "[", "]");
  private static String getMetricName(DataSourceWithSegmentSpec spec)
  {
    JOINER.add(getMetricName(spec.getDataSource())).add("=");
    JOINER.add(spec.getQuerySegmentSpec().getIntervals().stream()
                   .map(Interval::toString)
                   .collect(Collectors.joining(",", "[", "]"))
    );
    return JOINER.toString();
  }
}
