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

package org.apache.druid.query.movingaverage;

import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;

import java.util.ArrayList;
import java.util.List;

/**
 * Groups all the rows for a specific period together.
 * Rows of each period are placed in a single {@link RowBucket} (timed through the dateTime field).
 * (Assumpltion: Input arrives sorted by timestamp).
 */
public class BucketingAccumulator extends YieldingAccumulator<RowBucket, Row>
{

  /* (non-Javadoc)
   * @see YieldingAccumulator#accumulate(java.lang.Object, java.lang.Object)
   */
  @Override
  public RowBucket accumulate(RowBucket accumulated, Row in)
  {
    List<Row> rows;

    if (accumulated == null) {
      // first row, initializing
      rows = new ArrayList<>();
      accumulated = new RowBucket(in.getTimestamp(), rows);
    } else if (accumulated.getNextBucket() != null) {
      accumulated = accumulated.getNextBucket();
    }

    if (!accumulated.getDateTime().equals(in.getTimestamp())) {
      // day change detected
      rows = new ArrayList<>();
      rows.add(in);
      RowBucket nextBucket = new RowBucket(in.getTimestamp(), rows);
      accumulated.setNextBucket(nextBucket);
      yield();
    } else {
      // still on the same day
      rows = accumulated.getRows();
      rows.add(in);
    }

    return accumulated;
  }

}
