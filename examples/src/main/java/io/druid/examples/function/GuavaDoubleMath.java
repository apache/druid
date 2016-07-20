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

package io.druid.examples.function;

import com.google.common.math.DoubleMath;
import io.druid.math.expr.BuiltinFunctions.SingleParam;
import io.druid.math.expr.BuiltinFunctions.TripleParam;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;

/**
 */
public class GuavaDoubleMath implements Function.Library
{
  class Factorial extends SingleParam
  {
    @Override
    public String name()
    {
      return "factorial";
    }

    @Override
    protected ExprEval eval(ExprEval x)
    {
      return ExprEval.of(DoubleMath.factorial(x.asInt()));
    }
  }

  class FuzzyCompare extends TripleParam
  {
    @Override
    public String name()
    {
      return "fuzzyCompare";
    }

    @Override
    protected ExprEval eval(ExprEval x, ExprEval y, ExprEval z)
    {
      return ExprEval.of(DoubleMath.fuzzyCompare(x.asDouble(), y.asDouble(), z.asDouble()));
    }
  }
}
