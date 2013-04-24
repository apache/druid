package com.metamx.druid.query.having;

import com.google.common.base.Function;
import com.metamx.druid.input.Row;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

/**
 * A "having" clause that filters aggregated value. This is similar to SQL's "having"
 * clause.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type")
@JsonSubTypes(value={
  @JsonSubTypes.Type(name="and", value=AndHavingSpec.class),
  @JsonSubTypes.Type(name="or", value=OrHavingSpec.class),
  @JsonSubTypes.Type(name="not", value=NotHavingSpec.class),
  @JsonSubTypes.Type(name="greaterThan", value=GreaterThanHavingSpec.class),
  @JsonSubTypes.Type(name="lessThan", value=LessThanHavingSpec.class),
  @JsonSubTypes.Type(name="equalTo", value=EqualToHavingSpec.class)
})
public interface HavingSpec {
  /**
   * Evaluates if a given row satisfies the having spec.
   *
   * @param row A Row of data that may contain aggregated values
   *
   * @return true if the given row satisfies the having spec. False otherwise.
   *
   * @see Row
   */
  public boolean eval(Row row);

  // Atoms for easy combination, but for now they are mostly useful
  // for testing.
  /**
   * A "having" spec that always evaluates to false
   */
  public static final HavingSpec NEVER = new HavingSpec()
  {
    @Override
    public boolean eval(Row row)
    {
      return false;
    }
  };

  /**
   * A "having" spec that always evaluates to true
   */
  public static final HavingSpec ALWAYS = new HavingSpec(){
    @Override
    public boolean eval(Row row)
    {
      return true;
    }
  };
}
