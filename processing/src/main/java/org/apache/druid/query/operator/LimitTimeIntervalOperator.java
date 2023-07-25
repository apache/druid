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

package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.RowsAndColumnsDecorator;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;

public class LimitTimeIntervalOperator implements Operator
{
  private final Operator segmentOperator;
  private Interval interval;

  public LimitTimeIntervalOperator(
      Operator segmentOperator,
      Interval interval
  )
  {
    this.segmentOperator = segmentOperator;
    this.interval = interval;
  }

  @Nullable
  @Override
  public Closeable goOrContinue(
      Closeable continuationObject,
      Receiver receiver
  )
  {
    return segmentOperator.goOrContinue(continuationObject, new Receiver()
    {
      @Override
      public Signal push(RowsAndColumns rac)
      {
        if (Intervals.isEternity(interval)) {
          return receiver.push(rac);
        } else {
          final RowsAndColumnsDecorator decor = RowsAndColumnsDecorator.fromRAC(rac);
          decor.limitTimeRange(interval);
          return receiver.push(decor.toRowsAndColumns());
        }
      }

      @Override
      public void completed()
      {
        receiver.completed();
      }
    });
  }
}
