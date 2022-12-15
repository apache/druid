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
import org.apache.druid.collections.QueueBasedSorter;
import org.apache.druid.collections.Sorter;
import org.apache.druid.java.util.common.UOE;
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

    Comparator<Object[]> comparator = query.getGenericResultOrdering();
    Sorter<Object> sorter = new QueueBasedSorter<>(scanRowsLimit, comparator);
    List<String> columns = new ArrayList<>();
    while (!yielder.isDone()) {
      ScanResultValue srv = yielder.get();
      columns = columns.isEmpty() ? srv.getColumns() : columns;
      List<Object[]> events = (List<Object[]>) srv.getEvents();
      for (Object event : events) {
        //During the merge phase, there are 3 result types to consider
        if (event instanceof LinkedHashMap) {
          sorter.add(((LinkedHashMap) event).values().toArray());
        } else if (event instanceof Object[]){
          sorter.add((Object[]) event);
        }else {
          sorter.add(((List<Object>) event).toArray());
        }
      }
      yielder = yielder.next(null);
      count++;
    }
    final List<Object[]> sortedElements = new ArrayList<>(sorter.size());
    Iterators.addAll(sortedElements, sorter.drainElement());

    if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
      List<Map<String, Object>> events = new ArrayList<>(sortedElements.size());
      for (Object[] event : sortedElements) {
        Map<String, Object> eventMap = CollectionUtils.newLinkedHashMapWithExpectedSize(columns.size());
        events.add(eventMap);
        for (int j = 0; j < columns.size(); j++) {
          eventMap.put(columns.get(j), event[j]);
        }
      }
      return new ScanResultValue(null, columns, events);
    }
    return new ScanResultValue(null, columns, sortedElements.stream().map(e -> Arrays.asList(e)).collect(Collectors.toList()));
  }
}
