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

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.io.IOException;
import java.util.NoSuchElementException;

public class SequenceOperator implements Operator
{
  private final Sequence<RowsAndColumns> child;
  private Yielder<RowsAndColumns> yielder;
  private boolean closed = false;

  public SequenceOperator(
      Sequence<RowsAndColumns> child
  )
  {
    this.child = child;
  }

  @Override
  public void open()
  {
    if (closed) {
      throw new ISE("Operator closed, cannot be re-opened");
    }
    yielder = Yielders.each(child);
  }

  @Override
  public RowsAndColumns next()
  {
    if (closed) {
      throw new NoSuchElementException();
    }
    final RowsAndColumns retVal = yielder.get();
    yielder = yielder.next(null);
    return retVal;
  }

  @Override
  public boolean hasNext()
  {
    return !closed && !yielder.isDone();
  }

  @Override
  public void close(boolean cascade)
  {
    if (closed) {
      return;
    }
    try {
      yielder.close();
    }
    catch (IOException e) {
      throw new RE(e, "Exception when closing yielder from Sequence");
    }
    finally {
      closed = true;
    }
  }
}
