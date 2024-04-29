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

package org.apache.druid.sql.calcite;

import com.google.common.base.Supplier;
import org.apache.druid.common.config.NullHandling;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Collection of conditional disabler rules.
 */
class DisableUnless
{
  public static final DisableUnlessRule SQL_COMPATIBLE = new DisableUnlessRule(
      "NullHandling::sqlCompatible", NullHandling::sqlCompatible
  );

  public static class DisableUnlessRule implements ExecutionCondition
  {
    private Supplier<Boolean> predicate;
    private String message;

    public DisableUnlessRule(String message, Supplier<Boolean> predicate)
    {
      this.message = message;
      this.predicate = predicate;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context)
    {
      if (predicate.get()) {
        return ConditionEvaluationResult.enabled("condition not met");
      } else {
        return ConditionEvaluationResult.disabled(message);
      }
    }
  }
}
