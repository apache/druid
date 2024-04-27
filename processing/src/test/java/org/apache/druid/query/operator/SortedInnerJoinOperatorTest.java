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

import org.apache.druid.query.operator.join.JoinConfig;
import org.apache.druid.query.operator.join.JoinPartDefn;
import org.apache.druid.query.operator.join.SortedInnerJoinOperator;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;

public class SortedInnerJoinOperatorTest
{

  @Test
  public void testSimpleTwoWayJoin()
  {
    SortedInnerJoinOperator joinMe = new SortedInnerJoinOperator(
        Arrays.asList(
            buildDefn(
                MapOfColumnsRowsAndColumns
                    .builder()
                    .add("joinField", new int[]{0, 0, 0, 1, 1, 2, 4, 4, 4})
                    .add("projectMe", new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
                    .build()
            )
                .joinOn("joinField")
                .project("joinField", "projectMe")
                .build(),
            buildDefn(
                MapOfColumnsRowsAndColumns
                    .builder()
                    .add("joinField", new int[]{0, 1, 4})
                    .add("projectMeToo", ColumnType.STRING, "a", "b", "e")
                    .build()
            )
                .joinOn("joinField")
                .project("projectMeToo")
                .build()
        ),
        new JoinConfig(4)
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("joinField", new int[]{0, 0, 0, 1, 1})
                .expectColumn("projectMe", new int[]{3, 54, 21, 1, 5})
                .expectColumn("projectMeToo", ColumnType.STRING, "a", "a", "a", "b", "b")
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("joinField", new int[]{4, 4, 4})
                .expectColumn("projectMe", new int[]{2, 3, 92})
                .expectColumn("projectMeToo", ColumnType.STRING, "e", "e", "e")
                .allColumnsRegistered()
        )
        .runToCompletion(joinMe);
  }

  @Test
  public void testSimpleThreeWayJoin()
  {
    SortedInnerJoinOperator joinMe = new SortedInnerJoinOperator(
        Arrays.asList(
            buildDefn(
                MapOfColumnsRowsAndColumns
                    .builder()
                    .add("joinField", new double[]{0, 0, 0, 1, 1, 2, 4, 4, 4})
                    .add("projectMe", new int[]{3, 54, 21, 1, 5, 54, 2, 3, 92})
                    .build()
            )
                .joinOn("joinField")
                .project("joinField", "projectMe")
                .build(),
            buildDefn(
                MapOfColumnsRowsAndColumns
                    .builder()
                    .add("joinField", new double[]{0, 1, 4})
                    .add("projectMeToo", ColumnType.STRING, "a", "b", "e")
                    .build()
            )
                .joinOn("joinField")
                .project("projectMeToo")
                .build(),
            buildDefn(
                MapOfColumnsRowsAndColumns
                    .builder()
                    .add("joinField", new double[]{0, 0, 1, 2, 4})
                    .add("projectMeThree", ColumnType.STRING, "C", "CC", "B", "Z", "A")
                    .build()
            )
                .joinOn("joinField")
                .project("projectMeThree")
                .build()
        ),
        new JoinConfig(4)
    );

    new OperatorTestHelper()
        .expectRowsAndColumns(
            new RowsAndColumnsHelper()
                .expectColumn("joinField", new double[]{0, 0, 0, 0, 0, 0})
                .expectColumn("projectMe", new int[]{3, 3, 54, 54, 21, 21})
                .expectColumn("projectMeToo", ColumnType.STRING, "a", "a", "a", "a", "a", "a")
                .expectColumn("projectMeThree", ColumnType.STRING, "C", "CC", "C", "CC", "C", "CC")
                .allColumnsRegistered(),
            new RowsAndColumnsHelper()
                .expectColumn("joinField", new double[]{1, 1, 4, 4, 4})
                .expectColumn("projectMe", new int[]{1, 5, 2, 3, 92})
                .expectColumn("projectMeToo", ColumnType.STRING, "b", "b", "e", "e", "e")
                .expectColumn("projectMeThree", ColumnType.STRING, "B", "B", "A", "A", "A")
                .allColumnsRegistered()
        )
        .runToCompletion(joinMe);
  }

  private JoinPartDefn.Builder buildDefn(
      RowsAndColumns... racs
  )
  {
    return JoinPartDefn.builder(InlineScanOperator.make(racs));
  }
}
