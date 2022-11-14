package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class NaivePartitioningOperatorFactory implements OperatorFactory
{
  private final List<String> partitionColumns;

  @JsonCreator
  public NaivePartitioningOperatorFactory(
      @JsonProperty("partitionColumns") List<String> partitionColumns
  )
  {
    this.partitionColumns = partitionColumns;
  }

  @JsonProperty("partitionColumns")
  public List<String> getPartitionColumns()
  {
    return partitionColumns;
  }

  @Override
  public Operator wrap(Operator op)
  {
    return new NaivePartitioningOperator(partitionColumns, op);
  }
}
