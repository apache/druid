package com.metamx.druid;

import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.result.Result;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 */
public class CombiningIterableTest
{
  @Test
  public void testMerge()
  {
    List<Result<Object>> resultsBefore = Arrays.asList(
        new Result<Object>(new DateTime("2011-01-01"), 1L),
        new Result<Object>(new DateTime("2011-01-01"), 2L)
    );

    Iterable<Result<Object>> expectedResults = Arrays.<Result<Object>>asList(
        new Result<Object>(new DateTime("2011-01-01"), 3L)
    );

    Iterable<Result<Object>> resultsAfter = CombiningIterable.create(
        resultsBefore,
        new Comparator<Result<Object>>()
        {
          @Override
          public int compare(Result<Object> r1, Result<Object> r2)
          {
            return r1.getTimestamp().compareTo(r2.getTimestamp());
          }
        },
        new BinaryFn<Result<Object>, Result<Object>, Result<Object>>()
        {
          @Override
          public Result<Object> apply(final Result<Object> arg1, final Result<Object> arg2)
          {
            if (arg1 == null) {
              return arg2;
            }

            if (arg2 == null) {
              return arg1;
            }

            return new Result<Object>(arg1.getTimestamp(), ((Long) arg1.getValue()).longValue() + ((Long) arg2.getValue()).longValue());
          }
        }
    );

    Iterator<Result<Object>> it1 = expectedResults.iterator();
    Iterator<Result<Object>> it2 = resultsAfter.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      Result r1 = it1.next();
      Result r2 = it2.next();

      Assert.assertEquals(r1.getTimestamp(), r2.getTimestamp());
      Assert.assertEquals(r1.getValue(), r2.getValue());
    }
  }
}