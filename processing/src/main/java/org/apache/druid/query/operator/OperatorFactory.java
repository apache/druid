package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A factory for Operators.  This class exists to encapsulate the user-definition of an Operator. I.e. which operator,
 * what fields it should operate on, etc. etc.  These Factory objects are then used to combine Operators together
 * and run against concrete data.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "naivePartition", value = NaivePartitioningOperatorFactory.class),
    @JsonSubTypes.Type(name = "window", value = WindowOperatorFactory.class),
})
public interface OperatorFactory
{
  /**
   * Builds an operator according to the definition of the OperatorFactory and wraps it around the operator passed
   * in to this function.
   *
   * @param op the Operator to wrap
   * @return the wrapped Operator
   */
  Operator wrap(Operator op);

  /**
   * Validates the equivalence of Operators.  This is similar to @{code .equals} but is its own method
   * so that it can ignore certain fields that would be important for a true equality check.  Namely, two Operators
   * defined the same way but with different output names can be considered equivalent even though they are not equal.
   *
   * This primarily exists to simplify tests, where this equivalence can be used to validate that the Operators
   * created by the SQL planner are actually equivalent to what we expect without needing to be overly dependent on
   * how the planner names output columns
   *
   * @param other the processor to test equivalence of
   * @return boolean identifying if these processors should be considered equivalent to each other.
   */
  boolean validateEquivalent(OperatorFactory other);
}
