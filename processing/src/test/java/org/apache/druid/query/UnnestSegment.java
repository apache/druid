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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.UnnestStorageAdapter;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedHashSet;


/**
 * The segment for the Unnest Data Source.
 * The input column name, output name and the allowSet follow from {@link UnnestDataSource}
 */

public class UnnestSegment implements Segment
{
  private static final Logger log = new Logger(UnnestSegment.class);

  private final Segment baseSegment;
  private final String dimension;
  private final String renamedOutputDimension;
  private final LinkedHashSet<String> allowSet;

  public UnnestSegment(Segment baseSegment, String dimension, String outputName, LinkedHashSet<String> allowList)
  {
    this.baseSegment = baseSegment;
    this.dimension = dimension;
    this.renamedOutputDimension = outputName;
    this.allowSet = allowList;
  }

  @Override
  public SegmentId getId()
  {
    return baseSegment.getId();
  }

  @Override
  public Interval getDataInterval()
  {
    return baseSegment.getDataInterval();
  }

  @Nullable
  @Override
  public QueryableIndex asQueryableIndex()
  {
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    return new UnnestStorageAdapter(
        baseSegment.asStorageAdapter(),
        dimension,
        renamedOutputDimension,
        allowSet
    );
  }

  @Override
  public void close() throws IOException
  {
    baseSegment.close();
  }
}

