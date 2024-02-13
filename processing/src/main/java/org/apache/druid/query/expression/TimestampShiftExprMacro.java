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

package org.apache.druid.query.expression;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.vector.CastToTypeVectorProcessor;
import org.apache.druid.math.expr.vector.ExprVectorProcessor;
import org.apache.druid.math.expr.vector.LongOutLongInFunctionVectorValueProcessor;
import org.joda.time.Chronology;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class TimestampShiftExprMacro implements ExprMacroTable.ExprMacro
{
  private static final String FN_NAME = "timestamp_shift";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    validationHelperCheckArgumentRange(args, 3, 4);

    if (args.stream().skip(1).allMatch(Expr::isLiteral)) {
      return new TimestampShiftExpr(this, args);
    } else {
      // Use dynamic impl if any args are non-literal. Don't bother optimizing for the case where period is
      // literal but step isn't.
      return new TimestampShiftDynamicExpr(this, args);
    }
  }

  private static Period getPeriod(final List<Expr> args, final Expr.ObjectBinding bindings)
  {
    return new Period(args.get(1).eval(bindings).asString());
  }

  private static int getStep(final List<Expr> args, final Expr.ObjectBinding bindings)
  {
    return args.get(2).eval(bindings).asInt();
  }

  private static ISOChronology getTimeZone(final List<Expr> args, final Expr.ObjectBinding bindings)
  {
    final Expr timeZoneArg = args.size() > 3 ? args.get(3) : null;
    if (timeZoneArg == null) {
      return ISOChronology.getInstance(null);
    } else {
      final String zone = timeZoneArg.eval(bindings).asString();
      return ISOChronology.getInstance(zone != null ? DateTimes.inferTzFromString(zone) : null);
    }
  }

  private static class TimestampShiftExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    private final Chronology chronology;
    private final Period period;
    private final int step;

    TimestampShiftExpr(final TimestampShiftExprMacro macro, final List<Expr> args)
    {
      super(macro, args);
      period = getPeriod(args, InputBindings.nilBindings());
      chronology = getTimeZone(args, InputBindings.nilBindings());
      step = getStep(args, InputBindings.nilBindings());
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      ExprEval timestamp = args.get(0).eval(bindings);
      if (timestamp.isNumericNull()) {
        return ExprEval.of(null);
      }
      return ExprEval.of(chronology.add(period, timestamp.asLong(), step));
    }

    @Override
    public boolean canVectorize(InputBindingInspector inspector)
    {
      return args.get(0).canVectorize(inspector);
    }

    @Override
    public <T> ExprVectorProcessor<T> asVectorProcessor(VectorInputBindingInspector inspector)
    {
      ExprVectorProcessor<?> processor;
      processor = new LongOutLongInFunctionVectorValueProcessor(
          CastToTypeVectorProcessor.cast(args.get(0).asVectorProcessor(inspector), ExpressionType.LONG),
          inspector.getMaxVectorSize()
      )
      {
        @Override
        public long apply(long input)
        {
          return chronology.add(period, input, step);
        }
      };

      return (ExprVectorProcessor<T>) processor;
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.LONG;
    }
  }

  private static class TimestampShiftDynamicExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr
  {
    TimestampShiftDynamicExpr(final TimestampShiftExprMacro macro, final List<Expr> args)
    {
      super(macro, args);
    }

    @Nonnull
    @Override
    public ExprEval eval(final ObjectBinding bindings)
    {
      ExprEval timestamp = args.get(0).eval(bindings);
      if (timestamp.isNumericNull()) {
        return ExprEval.of(null);
      }
      final Period period = getPeriod(args, bindings);
      final Chronology chronology = getTimeZone(args, bindings);
      final int step = getStep(args, bindings);
      return ExprEval.of(chronology.add(period, timestamp.asLong(), step));
    }

    @Nullable
    @Override
    public ExpressionType getOutputType(InputBindingInspector inspector)
    {
      return ExpressionType.LONG;
    }

  }
}
