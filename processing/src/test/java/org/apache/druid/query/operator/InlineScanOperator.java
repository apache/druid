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
import com.google.common.collect.Iterators;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.Iterator;
import java.util.List;

public class InlineScanOperator implements Operator
{
  public static InlineScanOperator make(RowsAndColumns item)
  {
    return new InlineScanOperator(Iterators.singletonIterator(item));
  }

  public static InlineScanOperator make(List<RowsAndColumns> items)
  {
    return new InlineScanOperator(items.iterator());
  }

  private Iterator<RowsAndColumns> iter;

  public InlineScanOperator(
      Iterator<RowsAndColumns> iter
  )
  {
    Preconditions.checkNotNull(iter);
    this.iter = iter;
  }

  @Override
  public void open()
  {
  }

  @Override
  public RowsAndColumns next()
  {
    return iter.next();
  }

  @Override
  public boolean hasNext()
  {
    return iter.hasNext();
  }

  @Override
  public void close(boolean cascade)
  {
    iter = null;
  }
}
