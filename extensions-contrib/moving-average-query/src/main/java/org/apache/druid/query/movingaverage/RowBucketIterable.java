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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * It is the iterable used to bucket data into days,
 * doing appropriate lookahead to see if the next row is in the same day or a new day.
 */
public class RowBucketIterable implements Iterable<RowBucket>
{

  public final Sequence<Row> seq;
  private List<Interval> intervals;
  private Period period;

  public RowBucketIterable(Sequence<Row> seq, List<Interval> intervals, Period period)
  {
    this.seq = seq;
    this.period = period;
    this.intervals = intervals;
  }

  /* (non-Javadoc)
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<RowBucket> iterator()
  {
    return new RowIterator(seq, intervals, period);
  }

  static class RowIterator implements Iterator<RowBucket>
  {
    private Yielder<RowBucket> yielder;
    private boolean done = false;
    private DateTime endTime;
    private DateTime expectedBucket;
    private Period period;
    private int intervalIndex = 0;
    private List<Interval> intervals;
    private boolean processedLastRow = false;
    private boolean processedExtraRow = false;

    public RowIterator(Sequence<Row> rows, List<Interval> intervals, Period period)
    {
      this.period = period;
      this.intervals = intervals;
      expectedBucket = intervals.get(intervalIndex).getStart();
      endTime = intervals.get(intervals.size() - 1).getEnd();
      yielder = rows.toYielder(null, new BucketingAccumulator());
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext()
    {
      // expectedBucket < endTime
      if (expectedBucket.compareTo(endTime) < 0) {
        return true;
      }
      return false;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public RowBucket next()
    {
      RowBucket currentBucket = yielder.get();

      if (expectedBucket.compareTo(intervals.get(intervalIndex).getEnd()) >= 0) {
        intervalIndex++;
        if (intervalIndex <= intervals.size()) {
          expectedBucket = intervals.get(intervalIndex).getStart();
        }
      }
      // currentBucket > expectedBucket
      if (currentBucket != null && currentBucket.getDateTime().compareTo(expectedBucket) > 0) {
        currentBucket = new RowBucket(expectedBucket, Collections.emptyList());
        expectedBucket = expectedBucket.plus(period);
        return currentBucket;
      }

      if (!yielder.isDone()) {
        // standard case. return regular row
        yielder = yielder.next(currentBucket);
        expectedBucket = expectedBucket.plus(period);
        return currentBucket;
      } else if (!processedLastRow && yielder.get() != null && yielder.get().getNextBucket() == null) {
        // yielder.isDone, processing last row
        processedLastRow = true;
        expectedBucket = expectedBucket.plus(period);
        return currentBucket;
      } else if (!processedExtraRow && yielder.get() != null && yielder.get().getNextBucket() != null) {
        RowBucket lastRow = yielder.get().getNextBucket();

        if (lastRow.getDateTime().compareTo(expectedBucket) > 0) {
          lastRow = new RowBucket(expectedBucket, Collections.emptyList());
          expectedBucket = expectedBucket.plus(period);
          return lastRow;
        }

        // yielder is done, processing newBucket
        processedExtraRow = true;
        expectedBucket = expectedBucket.plus(period);
        return lastRow;
      } else if (expectedBucket.compareTo(endTime) < 0) {
        // add any trailing blank rows
        currentBucket = new RowBucket(expectedBucket, Collections.emptyList());
        expectedBucket = expectedBucket.plus(period);
        return currentBucket;
      } else {
        // we should never get here
        throw new NoSuchElementException();
      }

    }
  }

}
