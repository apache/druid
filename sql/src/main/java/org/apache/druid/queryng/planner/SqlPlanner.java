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

package org.apache.druid.queryng.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.sql.ProjectResultsOperatorEx;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.List;

public class SqlPlanner
{
  public static Operator<Object[]> projectResults(
      final FragmentContext context,
      final Sequence<Object[]> resultArrays,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper,
      final List<String> originalFields,
      final List<String> newFields,
      final List<SqlTypeName> newTypes
  )
  {
    // Build hash map for looking up original field positions, in case the number of fields is super high.
    final Object2IntMap<String> originalFieldsLookup = new Object2IntOpenHashMap<>();
    originalFieldsLookup.defaultReturnValue(-1);
    for (int i = 0; i < originalFields.size(); i++) {
      originalFieldsLookup.put(originalFields.get(i), i);
    }

    // Build "mapping" array of new field index -> old field index.
    final int[] mapping = new int[newFields.size()];
    for (int i = 0; i < newFields.size(); i++) {
      final String newField = newFields.get(i);
      final int idx = originalFieldsLookup.getInt(newField);
      if (idx < 0) {
        throw new ISE(
            "new field [%s] not contained in original fields [%s]",
            newField,
            String.join(", ", originalFields)
        );
      }

      mapping[i] = idx;
    }

    Operator<Object[]> inputOp = Operators.toOperator(context, resultArrays);
    return new ProjectResultsOperatorEx(
        context,
        inputOp,
        mapping,
        newTypes,
        jsonMapper,
        plannerContext.getTimeZone(),
        plannerContext.getPlannerConfig().shouldSerializeComplexValues(),
        plannerContext.isStringifyArrays());
  }
}
