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

package org.apache.druid.msq.logical;

import org.apache.druid.msq.input.InputSpec;

public abstract class DagInputSpec
{
  public abstract InputSpec toInputSpec();

  public static DagInputSpec of(LogicalStage inputStage)
  {
    return new DagStageInputSpec(inputStage);
  }

  public static DagInputSpec of(InputSpec inputSpec)
  {
    return new PhysicalInputSpec(inputSpec);
  }

  static class PhysicalInputSpec extends DagInputSpec
  {
    private InputSpec inputSpec;

    public PhysicalInputSpec(InputSpec inputSpec)
    {
      this.inputSpec = inputSpec;
    }

    @Override
    public InputSpec toInputSpec()
    {
      return inputSpec;
    }
  }

  static class DagStageInputSpec extends DagInputSpec {

    private LogicalStage inputStage;

    public DagStageInputSpec(LogicalStage inputStage)
    {
      this.inputStage = inputStage;
    }

    @Override
    public InputSpec toInputSpec()
    {
      if(true) {
        throw new RuntimeException("d");
      }
      return null;
    }

    public LogicalStage getStage()
    {
      return inputStage;
    }

  }
}
