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

import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.msq.input.stage.StageInputSpec;

import java.util.List;

/**
 * Utility functions for working with {@link InputSpec}.
 */
public class InputSpecs
{
  private InputSpecs()
  {
    // No instantiation.
  }

  public static IntSet getStageNumbers(final List<InputSpec> specs)
  {
    final IntSet retVal = new IntRBTreeSet();

    for (final InputSpec spec : specs) {
      if (spec instanceof StageInputSpec) {
        retVal.add(((StageInputSpec) spec).getStageNumber());
      }
    }

    return retVal;
  }
}
