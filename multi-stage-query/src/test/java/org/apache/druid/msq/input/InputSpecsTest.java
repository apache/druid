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

package org.apache.druid.msq.input;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InputSpecsTest
{
  @Test
  public void test_getStageNumbers()
  {
    Assertions.assertEquals(
        ImmutableSet.of(1, 2),
        InputSpecs.getStageNumbers(
            ImmutableList.of(
                new StageInputSpec(1),
                new StageInputSpec(2)
            )
        )
    );
  }

  @Test
  public void test_getHasLeafInputs_allStages()
  {
    Assertions.assertFalse(
        InputSpecs.hasLeafInputs(
            ImmutableList.of(
                new StageInputSpec(1),
                new StageInputSpec(2)
            ),
            IntSets.emptySet()
        )
    );
  }

  @Test
  public void test_getHasLeafInputs_broadcastTable()
  {
    Assertions.assertFalse(
        InputSpecs.hasLeafInputs(
            ImmutableList.of(new TableInputSpec("tbl", null, null)),
            IntSet.of(0)
        )
    );
  }

  @Test
  public void test_getHasLeafInputs_oneTableOneStage()
  {
    Assertions.assertTrue(
        InputSpecs.hasLeafInputs(
            ImmutableList.of(
                new TableInputSpec("tbl", null, null),
                new StageInputSpec(0)
            ),
            IntSets.emptySet()
        )
    );
  }

  @Test
  public void test_getHasLeafInputs_oneTableOneBroadcastStage()
  {
    Assertions.assertTrue(
        InputSpecs.hasLeafInputs(
            ImmutableList.of(
                new TableInputSpec("tbl", null, null),
                new StageInputSpec(0)
            ),
            IntSet.of(1)
        )
    );
  }

  @Test
  public void test_getHasLeafInputs_oneBroadcastTableOneStage()
  {
    Assertions.assertFalse(
        InputSpecs.hasLeafInputs(
            ImmutableList.of(
                new TableInputSpec("tbl", null, null),
                new StageInputSpec(0)
            ),
            IntSet.of(0)
        )
    );
  }

  @Test
  public void test_getHasLeafInputs_oneTableOneBroadcastTable()
  {
    Assertions.assertTrue(
        InputSpecs.hasLeafInputs(
            ImmutableList.of(
                new TableInputSpec("tbl", null, null),
                new TableInputSpec("tbl2", null, null)
            ),
            IntSet.of(1)
        )
    );
  }
}
