package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.Pair;

public class SerializablePairLongString extends Pair<Long, String>
{
  @JsonCreator
  public SerializablePairLongString(@JsonProperty("lhs") Long lhs, @JsonProperty("rhs") String rhs)
  {
    super(lhs, rhs);
  }

  @JsonProperty
  public Long getLhs()
  {
    return lhs;
  }

  @JsonProperty
  public String getRhs()
  {
    return rhs;
  }
}
