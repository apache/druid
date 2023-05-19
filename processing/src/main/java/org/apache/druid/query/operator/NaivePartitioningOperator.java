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
import org.apache.druid.query.rowsandcols.semantic.ClusteredGroupPartitioner;
import org.apache.druid.query.rowsandcols.semantic.DefaultClusteredGroupPartitioner;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

/**
 * This naive partitioning operator assumes that it's child operator always gives it RowsAndColumns objects that are
 * a superset of the partitions that it needs to provide.  It will never attempt to make a partition larger than a
 * single RowsAndColumns object that it is given from its child Operator.  A different operator should be used
 * if that is an important bit of functionality to have.
 * <p>
 * Additionally, this assumes that data has been pre-sorted according to the partitioning columns.  If it is
 * given data that has not been pre-sorted, an exception is expected to be thrown.
 */
public class NaivePartitioningOperator implements Operator
{
  private final List<String> partitionColumns;
  private final Operator child;

  private Iterator<RowsAndColumns> partitionsIter;

  public NaivePartitioningOperator(
      List<String> partitionColumns,
      Operator child
  )
  {
    this.partitionColumns = partitionColumns;
    this.child = child;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    return child.goOrContinue(
        continuation,
        new Receiver()
        {
          @Override
          public Signal push(RowsAndColumns rac)
          {
            ClusteredGroupPartitioner groupPartitioner = rac.as(ClusteredGroupPartitioner.class);
            if (groupPartitioner == null) {
              groupPartitioner = new DefaultClusteredGroupPartitioner(rac);
            }

            partitionsIter = groupPartitioner.partitionOnBoundaries(partitionColumns).iterator();

            Signal keepItGoing = Signal.GO;
            while (keepItGoing == Signal.GO && partitionsIter.hasNext()) {
              keepItGoing = receiver.push(partitionsIter.next());
            }
            return keepItGoing;
          }

          @Override
          public void completed()
          {
            receiver.completed();
          }
        }
    );
  }
}
