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

import com.google.common.base.Preconditions;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class InlineScanOperator implements Operator
{
  public static InlineScanOperator make(RowsAndColumns... item)
  {
    return new InlineScanOperator(Arrays.asList(item));
  }

  public static InlineScanOperator make(List<RowsAndColumns> items)
  {
    return new InlineScanOperator(items);
  }

  private Iterable<RowsAndColumns> iterable;

  public InlineScanOperator(
      Iterable<RowsAndColumns> iterable
  )
  {
    Preconditions.checkNotNull(iterable);
    this.iterable = iterable;
  }

  @Override
  public Closeable goOrContinue(Closeable continuation, Receiver receiver)
  {
    final Iterator<RowsAndColumns> iter;
    if (continuation == null) {
      iter = iterable.iterator();
    } else {
      iter = ((Continuation) continuation).iter;
    }

    Signal keepItGoing = Signal.GO;
    while (keepItGoing == Signal.GO && iter.hasNext()) {
      keepItGoing = receiver.push(iter.next());
    }
    if (keepItGoing == Signal.PAUSE && iter.hasNext()) {
      return new Continuation(iter);
    } else {
      receiver.completed();
      return null;
    }
  }

  private static class Continuation implements Closeable
  {
    private final Iterator<RowsAndColumns> iter;

    public Continuation(Iterator<RowsAndColumns> iter)
    {
      this.iter = iter;
    }

    @Override
    public void close()
    {
      // We don't actually have anything to close
    }
  }
}
