package com.metamx.druid.index.brita;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;
import java.util.regex.Pattern;


/**
 */
public class RegexFilter extends DimensionPredicateFilter
{
  public RegexFilter(
      String dimension,
      final String pattern
  )
  {
    super(
        dimension,
        new Predicate<String>()
        {
          Pattern compiled = Pattern.compile(pattern);

          @Override
          public boolean apply(@Nullable String input)
          {
            return compiled.matcher(input).find();
          }
        }
    );
  }
}
