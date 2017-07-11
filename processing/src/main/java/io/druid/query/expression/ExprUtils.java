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

package io.druid.query.expression;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.math.expr.Expr;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

public class ExprUtils
{
  private static final Expr.ObjectBinding NIL_BINDINGS = name -> null;

  public static Expr.ObjectBinding nilBindings()
  {
    return NIL_BINDINGS;
  }

  public static DateTimeZone toTimeZone(final Expr timeZoneArg)
  {
    if (!timeZoneArg.isLiteral()) {
      throw new IAE("Time zone must be a literal");
    }

    final Object literalValue = timeZoneArg.getLiteralValue();
    return literalValue == null ? DateTimeZone.UTC : DateTimeZone.forID((String) literalValue);
  }

  public static PeriodGranularity toPeriodGranularity(
      final Expr periodArg,
      final Expr originArg,
      final Expr timeZoneArg,
      final Expr.ObjectBinding bindings
  )
  {
    final Period period = new Period(periodArg.eval(bindings).asString());
    final DateTime origin;
    final DateTimeZone timeZone;

    if (originArg == null) {
      origin = null;
    } else {
      final Object value = originArg.eval(bindings).value();
      origin = value != null ? new DateTime(value) : null;
    }

    if (timeZoneArg == null) {
      timeZone = null;
    } else {
      final String value = timeZoneArg.eval(bindings).asString();
      timeZone = value != null ? DateTimeZone.forID(value) : null;
    }

    return new PeriodGranularity(period, origin, timeZone);
  }
}
