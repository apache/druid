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

import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ScanResultValueTimestampComparator implements Comparator<ScanResultValue>
{
  private final ScanQuery scanQuery;

  public ScanResultValueTimestampComparator(ScanQuery scanQuery)
  {
    this.scanQuery = scanQuery;
  }

  @Override
  public int compare(ScanResultValue o1, ScanResultValue o2)
  {
    int comparison;
    if (scanQuery.getResultFormat().equals(ScanQuery.RESULT_FORMAT_LIST)) {
      comparison = Longs.compare(
          (Long) ((Map<String, Object>) ((List) o1.getEvents()).get(0)).get(ColumnHolder.TIME_COLUMN_NAME),
          (Long) ((Map<String, Object>) ((List) o2.getEvents()).get(0)).get(ColumnHolder.TIME_COLUMN_NAME)
      );
    } else if (scanQuery.getResultFormat().equals(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)) {
      int val1TimeColumnIndex = o1.getColumns().indexOf(ColumnHolder.TIME_COLUMN_NAME);
      int val2TimeColumnIndex = o2.getColumns().indexOf(ColumnHolder.TIME_COLUMN_NAME);
      List<Object> event1 = (List<Object>) ((List<Object>) o1.getEvents()).get(0);
      List<Object> event2 = (List<Object>) ((List<Object>) o2.getEvents()).get(0);
      comparison = Longs.compare(
          (Long) event1.get(val1TimeColumnIndex),
          (Long) event2.get(val2TimeColumnIndex)
      );
    } else {
      throw new UOE("Result format [%s] is not supported", scanQuery.getResultFormat());
    }
    if (scanQuery.getTimeOrder().equals(ScanQuery.TIME_ORDER_DESCENDING)) {
      return comparison * -1;
    }
    return comparison;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ScanResultValueTimestampComparator comp = (ScanResultValueTimestampComparator) obj;
    return this.scanQuery.equals(comp.scanQuery);
  }
}
