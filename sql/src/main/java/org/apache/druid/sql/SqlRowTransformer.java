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

package org.apache.druid.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class transforms the values of TIMESTAMP or DATE type for sql query results.
 * The transformation is required only when the sql query is submitted to {@link org.apache.druid.sql.http.SqlResource}.
 */
public class SqlRowTransformer
{
  private final DateTimeZone timeZone;
  private final RelDataType rowType;
  private final List<String> fieldList;

  // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
  private final boolean[] timeColumns;
  private final boolean[] dateColumns;

  SqlRowTransformer(DateTimeZone timeZone, RelDataType rowType)
  {
    this.timeZone = timeZone;
    this.rowType = rowType;
    this.fieldList = new ArrayList<>(rowType.getFieldCount());
    this.timeColumns = new boolean[rowType.getFieldCount()];
    this.dateColumns = new boolean[rowType.getFieldCount()];
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      final SqlTypeName sqlTypeName = rowType.getFieldList().get(i).getType().getSqlTypeName();
      timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
      dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
      fieldList.add(rowType.getFieldList().get(i).getName());
    }
  }

  public RelDataType getRowType()
  {
    return rowType;
  }

  public List<String> getFieldList()
  {
    return fieldList;
  }

  @Nullable
  public Object transform(Object[] row, int i)
  {
    // If there's no Timeseries query, this check have no effect
    // Else if there's Timeseries query, the last row can be for the summary
    // result without a timestamp if grandTotal is passed in query context,
    // don't throw NPE
    if ((timeColumns[i] || dateColumns[i]) && row[i] == null) {
      return null;
    } else if (timeColumns[i]) {
      return ISODateTimeFormat.dateTime().print(
          Calcites.calciteTimestampToJoda((long) row[i], timeZone)
      );
    } else if (dateColumns[i]) {
      return ISODateTimeFormat.dateTime().print(
          Calcites.calciteDateToJoda((int) row[i], timeZone)
      );
    } else {
      return row[i];
    }
  }
}
