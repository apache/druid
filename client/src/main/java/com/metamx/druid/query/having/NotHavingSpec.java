package com.metamx.druid.query.having;

import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * The logical "not" operator for the "having" clause.
 */
public class NotHavingSpec implements HavingSpec
{
  private HavingSpec havingSpec;

  @JsonCreator
  public NotHavingSpec(@JsonProperty("havingSpec") HavingSpec havingSpec){
    this.havingSpec = havingSpec;
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("NotHavingSpec");
    sb.append("{havingSpec=").append(havingSpec);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NotHavingSpec that = (NotHavingSpec) o;

    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpec != null ? havingSpec.hashCode() : 0;
  }

  @JsonProperty("havingSpec")

  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @Override
  public boolean eval(Row row)
  {
    return !havingSpec.eval(row);
  }
}
