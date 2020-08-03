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

package org.apache.druid.query;

import org.apache.druid.timeline.Overshadowable;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Represents a source of data for a query obtained from multiple base tables. Implementations of this interface
 * must handle more than one table dataSource.
 */
public interface MultiTableDataSource extends DataSource
{
  /**
   * @param intervals     The intervals to find the timeline objects for
   * @param timelineMap   Table dataSource names along with its corresponding timeline for a specific interval
   * @param <ObjectType>  Type of the overshadowable object handled by the timeline
   * @return              Map of table datasources mapped to their corresponding list of timeline objects which needs to be queried
   */
  <ObjectType extends Overshadowable<ObjectType>> List<List<TimelineObjectHolder<String, ObjectType>>> retrieveSegmentsForIntervals(
      List<Interval> intervals,
      Map<String, TimelineLookup<String, ObjectType>> timelineMap,
      BiFunction<Interval, TimelineLookup<String, ObjectType>, List<TimelineObjectHolder<String, ObjectType>>> biFunction
  );

  /**
   * Returns the base table dataSources from which the data for a query is retrieved.
   */
  List<TableDataSource> getDataSources();
}
