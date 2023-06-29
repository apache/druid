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

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.NaiveSortMaker;

import java.io.Closeable;
import java.util.ArrayList;

/**
 * A naive sort operator is an operation that sorts a stream of data in-place.  Generally speaking this means
 * that it has to accumulate all of the data of its child operator first before it can sort.  This limitation
 * means that hopefully this operator is only planned in a very small number of circumstances.
 */
public class NaiveSortOperator implements Operator
{
  private final Operator child;
  private final ArrayList<ColumnWithDirection> sortColumns;

  public NaiveSortOperator(
      Operator child,
      ArrayList<ColumnWithDirection> sortColumns
  )
  {
    this.child = child;
    this.sortColumns = sortColumns;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    return child.goOrContinue(
        continuation,
        new Receiver()
        {
          NaiveSortMaker.NaiveSorter sorter = null;

          @Override
          public Signal push(RowsAndColumns rac)
          {
            if (sorter == null) {
              sorter = NaiveSortMaker.fromRAC(rac).make(sortColumns);
            } else {
              sorter.moreData(rac);
            }
            return Signal.GO;
          }

          @Override
          public void completed()
          {
            receiver.push(sorter.complete());
            receiver.completed();
          }
        }
    );
  }
}
