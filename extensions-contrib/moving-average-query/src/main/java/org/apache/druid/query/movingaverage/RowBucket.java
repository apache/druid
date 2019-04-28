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
import org.joda.time.DateTime;

import java.util.List;

/**
 * Represents a set of rows for a specific date
 * Each RowBucket is an element in a list (holds a pointer to the next RowBucket)
 */
public class RowBucket
{
  private final DateTime dateTime;
  private final List<Row> rows;
  private RowBucket nextBucket = null;

  public RowBucket(DateTime dateTime, List<Row> rows)
  {
    this.dateTime = dateTime;
    this.rows = rows;
  }

  public DateTime getDateTime()
  {
    return dateTime;
  }

  public List<Row> getRows()
  {
    return rows;
  }

  public RowBucket getNextBucket()
  {
    return nextBucket;
  }

  public void setNextBucket(RowBucket nextRow)
  {
    this.nextBucket = nextRow;
  }
}
