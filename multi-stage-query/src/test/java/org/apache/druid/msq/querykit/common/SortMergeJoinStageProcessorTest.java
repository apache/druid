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

package org.apache.druid.msq.querykit.common;

import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SortMergeJoinStageProcessorTest
{
  @Test
  public void test_validateCondition()
  {
    Assertions.assertNotNull(
        SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression("1", "j.", ExprMacroTable.nil())
        )
    );

    Assertions.assertNotNull(
        SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression("x == \"j.y\"", "j.", ExprMacroTable.nil())
        )
    );

    Assertions.assertNotNull(
        SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression("1", "j.", ExprMacroTable.nil())
        )
    );

    Assertions.assertNotNull(
        SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression("x == \"j.y\" && a == \"j.b\"", "j.", ExprMacroTable.nil())
        )
    );

    Assertions.assertNotNull(
        SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression(
                "notdistinctfrom(x, \"j.y\") && a == \"j.b\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression("x == y", "j.", ExprMacroTable.nil())
        )
    );

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SortMergeJoinStageProcessor.validateCondition(
            JoinConditionAnalysis.forExpression("x + 1 == \"j.y\"", "j.", ExprMacroTable.nil())
        )
    );
  }

  @Test
  public void test_toKeyColumns()
  {
    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("y", KeyOrder.ASCENDING))
        ),
        SortMergeJoinStageProcessor.toKeyColumns(
            JoinConditionAnalysis.forExpression(
                "x == \"j.y\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of(),
            ImmutableList.of()
        ),
        SortMergeJoinStageProcessor.toKeyColumns(
            JoinConditionAnalysis.forExpression(
                "1",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING), new KeyColumn("a", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("y", KeyOrder.ASCENDING), new KeyColumn("b", KeyOrder.ASCENDING))
        ),
        SortMergeJoinStageProcessor.toKeyColumns(
            JoinConditionAnalysis.forExpression(
                "x == \"j.y\" && a == \"j.b\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING), new KeyColumn("a", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("y", KeyOrder.ASCENDING), new KeyColumn("b", KeyOrder.ASCENDING))
        ),
        SortMergeJoinStageProcessor.toKeyColumns(
            JoinConditionAnalysis.forExpression(
                "x == \"j.y\" && notdistinctfrom(a, \"j.b\")",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING), new KeyColumn("a", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("y", KeyOrder.ASCENDING), new KeyColumn("b", KeyOrder.ASCENDING))
        ),
        SortMergeJoinStageProcessor.toKeyColumns(
            JoinConditionAnalysis.forExpression(
                "notdistinctfrom(x, \"j.y\") && a == \"j.b\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertEquals(
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("x", KeyOrder.ASCENDING), new KeyColumn("a", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("y", KeyOrder.ASCENDING), new KeyColumn("b", KeyOrder.ASCENDING))
        ),
        SortMergeJoinStageProcessor.toKeyColumns(
            JoinConditionAnalysis.forExpression(
                "notdistinctfrom(x, \"j.y\") && notdistinctfrom(a, \"j.b\")",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );
  }

  @Test
  public void test_toRequiredNonNullKeyParts()
  {
    Assertions.assertArrayEquals(
        new int[0],
        SortMergeJoinStageProcessor.toRequiredNonNullKeyParts(
            JoinConditionAnalysis.forExpression(
                "1",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertArrayEquals(
        new int[]{0},
        SortMergeJoinStageProcessor.toRequiredNonNullKeyParts(
            JoinConditionAnalysis.forExpression(
                "x == \"j.y\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertArrayEquals(
        new int[]{0, 1},
        SortMergeJoinStageProcessor.toRequiredNonNullKeyParts(
            JoinConditionAnalysis.forExpression(
                "x == \"j.y\" && a == \"j.b\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertArrayEquals(
        new int[]{0},
        SortMergeJoinStageProcessor.toRequiredNonNullKeyParts(
            JoinConditionAnalysis.forExpression(
                "x == \"j.y\" && notdistinctfrom(a, \"j.b\")",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertArrayEquals(
        new int[]{1},
        SortMergeJoinStageProcessor.toRequiredNonNullKeyParts(
            JoinConditionAnalysis.forExpression(
                "notdistinctfrom(x, \"j.y\") && a == \"j.b\"",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );

    Assertions.assertArrayEquals(
        new int[0],
        SortMergeJoinStageProcessor.toRequiredNonNullKeyParts(
            JoinConditionAnalysis.forExpression(
                "notdistinctfrom(x, \"j.y\") && notdistinctfrom(a, \"j.b\")",
                "j.",
                ExprMacroTable.nil()
            )
        )
    );
  }
}
