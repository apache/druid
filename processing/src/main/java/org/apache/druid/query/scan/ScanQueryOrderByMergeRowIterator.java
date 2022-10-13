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
import com.google.common.collect.Ordering;
import org.apache.druid.collections.QueueBasedSorter;
import org.apache.druid.collections.Sorter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.utils.CollectionUtils;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScanQueryOrderByMergeRowIterator extends ScanQueryLimitRowIterator
{

  public ScanQueryOrderByMergeRowIterator(
      QueryRunner<ScanResultValue> baseRunner,
      QueryPlus<ScanResultValue> queryPlus,
      ResponseContext responseContext
  )
  {
    super(baseRunner, queryPlus, responseContext);
    if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
      throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
    }
  }

  @Override
  public boolean hasNext()
  {
    return !yielder.isDone();
  }

  @Override
  public ScanResultValue next()
  {

    final int scanRowsLimit;
    if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
      scanRowsLimit = Integer.MAX_VALUE;
    } else {
      scanRowsLimit = Math.toIntExact(query.getScanRowsLimit());
    }

    List<String> orderByDirection = query.getOrderBys()
                                         .stream()
                                         .map(orderBy -> orderBy.getOrder().toString())
                                         .collect(Collectors.toList());
    List<Integer> sortColumnIdxs = query.getSortColumnIdxs();

    Ordering<Comparable>[] orderings = new Ordering[orderByDirection.size()];
    for (int i = 0; i < orderByDirection.size(); i++) {
      orderings[i] = ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))
                     ? Comparators.naturalNullsFirst()
                     : Comparators.<Comparable>naturalNullsFirst().reverse();
    }

    Comparator<List<Object>> comparator = (o1, o2) -> {
      for (int i = 0; i < sortColumnIdxs.size(); i++) {
        int compare = orderings[i].compare(
            (Comparable) o1.get(sortColumnIdxs.get(i)),
            (Comparable) o2.get(sortColumnIdxs.get(i))
        );
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    };
    Sorter<Object> sorter = new QueueBasedSorter<Object>(scanRowsLimit, comparator);
    List<String> columns = new ArrayList<>();
    while (!yielder.isDone()) {
      ScanResultValue srv = yielder.get();
      columns = columns.isEmpty() ? srv.getColumns() : columns;
      List<List<Object>> events = (List<List<Object>>) srv.getEvents();
      for (Object event : events) {
        if (event instanceof LinkedHashMap) {
          sorter.add(Arrays.asList(((LinkedHashMap) event).values().toArray()));
        } else {
          sorter.add((List<Object>) event);
        }
      }
      yielder = yielder.next(null);
      count++;
    }
    final List<List<Object>> sortedElements = new ArrayList<>(sorter.size());
    Iterators.addAll(sortedElements, sorter.drainElement());

    if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
      List<Map<String, Object>> events = new ArrayList<>(sortedElements.size());
      for (List<Object> event : sortedElements) {
        Map<String, Object> eventMap = CollectionUtils.newLinkedHashMapWithExpectedSize(columns.size());
        events.add(eventMap);
        for (int j = 0; j < columns.size(); j++) {
          eventMap.put(columns.get(j), event.get(j));
        }
      }
      return new ScanResultValue(null, columns, events);
    }
    return new ScanResultValue(null, columns, sortedElements);
  }
}
