package com.metamx.druid.query.group.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.input.Row;

import java.util.List;

/**
 * The logical "or" operator for the "having" clause.
 */
public class OrHavingSpec implements HavingSpec
{
  private List<HavingSpec> havingSpecs;

  @JsonCreator
  public OrHavingSpec(@JsonProperty("havingSpecs") List<HavingSpec> havingSpecs) {
    this.havingSpecs = havingSpecs == null ? ImmutableList.<HavingSpec>of() : havingSpecs;
  }

  @JsonProperty("havingSpecs")
  public List<HavingSpec> getHavingSpecs(){
    return havingSpecs;
  }

  @Override
  public boolean eval(Row row)
  {
    for(HavingSpec havingSpec: havingSpecs) {
      if(havingSpec.eval(row)){
        return true;
      }
    }

    return false;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OrHavingSpec that = (OrHavingSpec) o;

    if (havingSpecs != null ? !havingSpecs.equals(that.havingSpecs) : that.havingSpecs != null) return false;

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
    sb.append("OrHavingSpec");
    sb.append("{havingSpecs=").append(havingSpecs);
    sb.append('}');
    return sb.toString();
  }
}
