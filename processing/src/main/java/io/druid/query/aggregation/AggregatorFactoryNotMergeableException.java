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

package io.druid.query.aggregation;

import io.druid.java.util.common.StringUtils;

/**
 */
public class AggregatorFactoryNotMergeableException extends Exception
{
  public AggregatorFactoryNotMergeableException()
  {
  }

  public AggregatorFactoryNotMergeableException(String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
  }

  public AggregatorFactoryNotMergeableException(Throwable cause, String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments), cause);
  }

  public AggregatorFactoryNotMergeableException(Throwable cause)
  {
    super(cause);
  }

  public AggregatorFactoryNotMergeableException(AggregatorFactory af1, AggregatorFactory af2)
  {
    this(
        "can't merge [%s : %s] and [%s : %s] , with detailed info [%s] and [%s]",
        af1.getName(),
        af1.getClass().getName(),
        af2.getName(),
        af2.getClass().getName(),
        af1,
        af2
    );
  }
}
