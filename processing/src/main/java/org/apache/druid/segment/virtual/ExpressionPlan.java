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

package org.apache.druid.segment.virtual;

import org.apache.druid.com.google.common.collect.Iterables;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprType;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class ExpressionPlan
{
  public enum Trait
  {
    /**
     * expression has no inputs and can be optimized into a constant selector
      */
    CONSTANT,
    /**
     * expression has a single, single valued input, and is dictionary encoded if the value is a string
     */
    SINGLE_INPUT_SCALAR,
    /**
     * expression has a single input, which may produce single or multi-valued output, but if so, it must be implicitly
     * mappable  (i.e. the expression is not treating its input as an array and not wanting to output an array)
     */
    SINGLE_INPUT_MAPPABLE,
    /**
     * expression must be implicitly mapped across the multiple values per row of known multi-value inputs
     */
    NEEDS_APPLIED,
    /**
     * expression has inputs whose type was unresolveable
     */
    UNKNOWN_INPUTS,
    /**
     * expression has inputs whose type was incomplete, such as unknown multi-valuedness
     */
    INCOMPLETE_INPUTS,
    /**
     * expression explicitly using multi-valued inputs as array inputs
     */
    NON_SCALAR_INPUTS,
    /**
     * expression produces explict multi-valued output, or implicit multi-valued output via mapping
     */
    NON_SCALAR_OUTPUT,
    /**
     * expression is vectorizable
     */
    VECTORIZABLE
  }

  private final Expr expression;
  private final Expr.BindingAnalysis analysis;
  private final EnumSet<Trait> traits;

  @Nullable
  private final ExprType outputType;
  @Nullable
  private final ValueType singleInputType;
  private final Set<String> unknownInputs;
  private final List<String> unappliedInputs;

  ExpressionPlan(
      Expr expression,
      Expr.BindingAnalysis analysis,
      EnumSet<Trait> traits,
      @Nullable ExprType outputType,
      @Nullable ValueType singleInputType,
      Set<String> unknownInputs,
      List<String> unappliedInputs
  )
  {
    this.expression = expression;
    this.analysis = analysis;
    this.traits = traits;
    this.outputType = outputType;
    this.singleInputType = singleInputType;
    this.unknownInputs = unknownInputs;
    this.unappliedInputs = unappliedInputs;
  }

  public boolean isConstant()
  {
    return analysis.getRequiredBindings().isEmpty();
  }

  public Expr getExpression()
  {
    return expression;
  }

  public Expr getAppliedExpression()
  {
    if (is(Trait.NEEDS_APPLIED)) {
      return Parser.applyUnappliedBindings(expression, analysis, unappliedInputs);
    }
    return expression;
  }

  public Expr.BindingAnalysis getAnalysis()
  {
    return analysis;
  }

  public boolean is(Trait... flags)
  {
    return is(traits, flags);
  }

  public boolean any(Trait... flags)
  {
    return any(traits, flags);
  }

  @Nullable
  public ExprType getOutputType()
  {
    return outputType;
  }

  @Nullable
  public ValueType getSingleInputType()
  {
    return singleInputType;
  }

  public String getSingleInputName()
  {
    return Iterables.getOnlyElement(analysis.getRequiredBindings());
  }

  public Set<String> getUnknownInputs()
  {
    return unknownInputs;
  }

  static boolean is(EnumSet<Trait> traits, Trait... args)
  {
    return Arrays.stream(args).allMatch(traits::contains);
  }

  static boolean any(EnumSet<Trait> traits, Trait... args)
  {
    return Arrays.stream(args).anyMatch(traits::contains);
  }

  static boolean none(EnumSet<Trait> traits, Trait... args)
  {
    return Arrays.stream(args).noneMatch(traits::contains);
  }
}
