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

package io.druid.indexer.path;

import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

/**
 */
public interface UsedSegmentLister
{
  /**
   * Get all segments which may include any data in the interval and are flagged as used.
   *
   * @param dataSource The datasource to query
   * @param interval   The interval for which all applicable and used datasources are requested. Start is inclusive, end is exclusive
   *
   * @return The DataSegments which include data in the requested interval. These segments may contain data outside the requested interval.
   *
   * @throws IOException
   */
  public List<DataSegment> getUsedSegmentsForInterval(final String dataSource, final Interval interval)
      throws IOException;
}
