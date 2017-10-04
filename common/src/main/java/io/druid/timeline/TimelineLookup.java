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

package io.druid.timeline;

import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.List;


public interface TimelineLookup<VersionType, ObjectType>
{

  /**
   * Does a lookup for the objects representing the given time interval.  Will *only* return
   * PartitionHolders that are complete.
   *
   * @param interval interval to find objects for
   *
   * @return Holders representing the interval that the objects exist for, PartitionHolders
   *         are guaranteed to be complete. Holders returned sorted by the interval.
   */
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval);

  /**
   * Does a lookup for the objects representing the given time interval.  Will also return
   * incomplete PartitionHolders.
   *
   * @param interval interval to find objects for
   *
   * @return Holders representing the interval that the objects exist for, PartitionHolders
   *         can be incomplete. Holders returned sorted by the interval.
   */
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookupWithIncompletePartitions(Interval interval);

  public PartitionHolder<ObjectType> findEntry(Interval interval, VersionType version);

}
