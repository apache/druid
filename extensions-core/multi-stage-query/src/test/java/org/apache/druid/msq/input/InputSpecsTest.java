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
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.junit.Assert;
import org.junit.Test;

public class InputSpecsTest
{
  @Test
  public void test_getStageNumbers()
  {
    Assert.assertEquals(
        ImmutableSet.of(1, 2),
        InputSpecs.getStageNumbers(
            ImmutableList.of(
                new StageInputSpec(1),
                new StageInputSpec(2)
            )
        )
    );
  }
}
