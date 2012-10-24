package com.metamx.druid.query.dimension;

import com.metamx.common.IAE;
import org.codehaus.jackson.annotate.JsonCreator;

import java.util.Map;

/**
 */
public class LegacyDimensionSpec extends DefaultDimensionSpec
{
  private static final String convertValue(Object dimension, String name)
  {
    final String retVal;

    if (dimension instanceof String) {
      retVal = (String) dimension;
    } else if (dimension instanceof Map) {
      retVal = (String) ((Map) dimension).get(name);
    } else {
      throw new IAE("Unknown type[%s] for dimension[%s]", dimension.getClass(), dimension);
    }

    return retVal;
  }

  @JsonCreator
  public LegacyDimensionSpec(Object dimension)
  {
    super(convertValue(dimension, "dimension"), convertValue(dimension, "outputName"));
  }
}
