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

package org.apache.druid.frame.key;

import java.util.Comparator;
import java.util.List;

/**
 * Comparator for {@link RowKey} instances.
 *
 * Delegates the comparing to a {@link ByteRowKeyComparator}.
 */
public class RowKeyComparator implements Comparator<RowKey>
{
  private final ByteRowKeyComparator byteRowKeyComparatorDelegate;

  private RowKeyComparator(final ByteRowKeyComparator byteRowKeyComparatorDelegate)
  {
    this.byteRowKeyComparatorDelegate = byteRowKeyComparatorDelegate;
  }

  public static RowKeyComparator create(final List<SortColumn> keyColumns)
  {
    return new RowKeyComparator(ByteRowKeyComparator.create(keyColumns));
  }

  @Override
  public int compare(final RowKey key1, final RowKey key2)
  {
    return byteRowKeyComparatorDelegate.compare(key1.array(), key2.array());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RowKeyComparator that = (RowKeyComparator) o;
    return byteRowKeyComparatorDelegate.equals(that.byteRowKeyComparatorDelegate);
  }

  @Override
  public int hashCode()
  {
    return byteRowKeyComparatorDelegate.hashCode();
  }

  @Override
  public String toString()
  {
    return "RowKeyComparator{" +
           "byteRowKeyComparatorDelegate=" + byteRowKeyComparatorDelegate +
           '}';
  }
}
