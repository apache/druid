package org.apache.druid.iceberg.filter;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class IcebergTimeWindowFilterTest
{
  @Test
  public void testFilter()
  {
    String intervalColumn = "eventTime";
    DateTime currentTimestamp = DateTimes.nowUtc();
    IcebergTimeWindowFilter intervalFilter = new IcebergTimeWindowFilter(intervalColumn, new Period("P2D").toStandardDuration(), new Period("P1D").toStandardDuration(), currentTimestamp);
    Expression expectedExpression = Expressions.and(
            Expressions.greaterThanOrEqual(
                intervalColumn,
                Literal.of((currentTimestamp.getMillis() - Duration.standardDays(2L).getMillis()) * 1000 )
                       .to(Types.TimestampType.withZone())
                       .value()
            ),
            Expressions.lessThanOrEqual(
                intervalColumn,
                Literal.of((currentTimestamp.getMillis() + Duration.standardDays(1L).getMillis()) * 1000 )
                       .to(Types.TimestampType.withZone())
                       .value()
            )
        );
    Assert.assertEquals(expectedExpression.toString(), intervalFilter.getFilterExpression().toString());
  }
}
