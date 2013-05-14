package com.metamx.druid.query.group.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.input.Row;

import java.util.List;

/**
 * The logical "and" operator for the "having" clause.
 */
public class AndHavingSpec implements HavingSpec
{
  private List<HavingSpec> havingSpecs;

  @JsonCreator
  public AndHavingSpec(@JsonProperty("havingSpecs") List<HavingSpec> havingSpecs)
  {
    this.havingSpecs = havingSpecs == null ? ImmutableList.<HavingSpec>of() : havingSpecs;
  }

  @JsonProperty("havingSpecs")
  public List<HavingSpec> getHavingSpecs()
  {
    return havingSpecs;
  }

  @Override
  public boolean eval(Row row)
  {
    for (HavingSpec havingSpec : havingSpecs) {
      if (!havingSpec.eval(row)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AndHavingSpec that = (AndHavingSpec) o;

    if (havingSpecs != null ? !havingSpecs.equals(that.havingSpecs) : that.havingSpecs != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpecs != null ? havingSpecs.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("AndHavingSpec");
    sb.append("{havingSpecs=").append(havingSpecs);
    sb.append('}');
    return sb.toString();
  }
}
