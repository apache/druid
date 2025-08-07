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

package org.apache.druid.msq.dart.controller;

import org.apache.druid.msq.dart.worker.DartQueryableSegment;
import org.apache.druid.msq.input.table.DataServerRequestDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the set of segments assigned to a particular dart worker, used by {@link DartTableInputSpecSlicer}.
 */
public class DartSegmentAssignment
{
  private final List<DartQueryableSegment> dartQueryableSegments;
  private final List<DataServerRequestDescriptor> dataServerRequestDescriptor;

  public DartSegmentAssignment(
      List<DartQueryableSegment> dartQueryableSegments,
      List<DataServerRequestDescriptor> dataServerRequestDescriptor
  )
  {
    this.dartQueryableSegments = dartQueryableSegments;
    this.dataServerRequestDescriptor = dataServerRequestDescriptor;
  }

  public static DartSegmentAssignment empty()
  {
    return new DartSegmentAssignment(new ArrayList<>(), new ArrayList<>());
  }

  public void addSegments(DartQueryableSegment segment)
  {
    dartQueryableSegments.add(segment);
  }

  public void addRequest(DataServerRequestDescriptor requestDescriptor)
  {
    dataServerRequestDescriptor.add(requestDescriptor);
  }

  public List<DartQueryableSegment> getDartQueryableSegments()
  {
    return dartQueryableSegments;
  }

  public List<DataServerRequestDescriptor> getDataServerRequestDescriptor()
  {
    return dataServerRequestDescriptor;
  }

  public boolean isEmpty()
  {
    return dataServerRequestDescriptor.isEmpty() && dartQueryableSegments.isEmpty();
  }
}
