/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.rel;

import io.druid.java.util.common.StringUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;

public class CannotBuildQueryException extends RuntimeException
{
  public CannotBuildQueryException(String message)
  {
    super(message);
  }

  public CannotBuildQueryException(RelNode relNode)
  {
    super(StringUtils.nonStrictFormat("Cannot process rel[%s]", relNode));
  }

  public CannotBuildQueryException(RelNode relNode, RexNode rexNode)
  {
    super(StringUtils.nonStrictFormat("Cannot translate reference[%s] from rel[%s]", rexNode, relNode));
  }

  public CannotBuildQueryException(RelNode relNode, AggregateCall aggregateCall)
  {
    super(StringUtils.nonStrictFormat("Cannot translate aggregator[%s] from rel[%s]", aggregateCall, relNode));
  }
}
