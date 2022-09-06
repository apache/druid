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

package org.apache.druid.query.scan;

import com.google.common.collect.Iterators;
import org.apache.druid.collections.MultiColumnSorter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ScanQueryOrderByLimitRowIterator extends ScanQueryLimitRowIterator
{

  public ScanQueryOrderByLimitRowIterator(
      QueryRunner<ScanResultValue> baseRunner,
      QueryPlus<ScanResultValue> queryPlus,
      ResponseContext responseContext
  )
  {
    super(baseRunner, queryPlus, responseContext);
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public ScanResultValue next()
  {
    if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
      throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
    }
    final int limit = Math.toIntExact(query.getScanRowsLimit());
    List<String> sortColumns = query.getOrderBys().stream().map(orderBy -> orderBy.getColumnName()).collect(Collectors.toList());
    List<String> orderByDirection = query.getOrderBys().stream().map(orderBy -> orderBy.getOrder().toString()).collect(Collectors.toList());
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Object>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Object>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Object> o1,
          MultiColumnSorter.MultiColumnSorterElement<Object> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            } else {
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter<Object> multiColumnSorter = new MultiColumnSorter<Object>(limit, comparator);

    List<String> columns = new ArrayList<>();
    while (!yielder.isDone()) {
      ScanResultValue srv = yielder.get();
      // Only replace once using the columns from the first event
      columns = columns.isEmpty() ? srv.getColumns() : columns;
      List<Integer> idxs = sortColumns.stream().map(c -> srv.getColumns().indexOf(c)).collect(Collectors.toList());
      List events = (List) (srv.getEvents());
      for (Object event : events) {
        List<Comparable> sortValues;
        if (event instanceof LinkedHashMap) {
          sortValues = sortColumns.stream().map(c -> ((LinkedHashMap<Object, Comparable>) event).get(c)).collect(Collectors.toList());
        } else {
          sortValues = idxs.stream().map(idx -> ((List<Comparable>) event).get(idx)).collect(Collectors.toList());
        }

        multiColumnSorter.add(event, sortValues);
      }

      yielder = yielder.next(null);
      count++;
    }
    final List<Object> sortedElements = new ArrayList<>(limit);
    Iterators.addAll(sortedElements, multiColumnSorter.drain());
    return new ScanResultValue(null, columns, sortedElements);
  }
}
