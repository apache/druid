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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.query.operator.window.WindowOperatorFactory;

/**
 * A factory for Operators.  This class exists to encapsulate the user-definition of an Operator. I.e. which operator,
 * what fields it should operate on, etc. etc.  These Factory objects are then used to combine Operators together
 * and run against concrete data.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "naivePartition", value = NaivePartitioningOperatorFactory.class),
    @JsonSubTypes.Type(name = "naiveSort", value = NaiveSortOperatorFactory.class),
    @JsonSubTypes.Type(name = "window", value = WindowOperatorFactory.class),
    @JsonSubTypes.Type(name = "scan", value = ScanOperatorFactory.class),
})
public interface OperatorFactory
{
  /**
   * Builds an operator according to the definition of the OperatorFactory and wraps it around the operator passed
   * in to this function.
   *
   * @param op the Operator to wrap
   * @return the wrapped Operator
   */
  Operator wrap(Operator op);

  /**
   * Validates the equivalence of Operators.  This is similar to @{code .equals} but is its own method
   * so that it can ignore certain fields that would be important for a true equality check.  Namely, two Operators
   * defined the same way but with different output names can be considered equivalent even though they are not equal.
   * <p>
   * This primarily exists to simplify tests, where this equivalence can be used to validate that the Operators
   * created by the SQL planner are actually equivalent to what we expect without needing to be overly dependent on
   * how the planner names output columns
   *
   * @param other the processor to test equivalence of
   * @return boolean identifying if these processors should be considered equivalent to each other.
   */
  boolean validateEquivalent(OperatorFactory other);
}
