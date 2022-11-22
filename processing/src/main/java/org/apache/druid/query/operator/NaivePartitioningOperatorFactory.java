package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class NaivePartitioningOperatorFactory implements OperatorFactory
{
  private final List<String> partitionColumns;

  @JsonCreator
  public NaivePartitioningOperatorFactory(
      @JsonProperty("partitionColumns") List<String> partitionColumns
  )
  {
    this.partitionColumns = partitionColumns == null ? new ArrayList<>() : partitionColumns;
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

  @Override
  public boolean validateEquivalent(OperatorFactory other)
  {
    if (other instanceof NaivePartitioningOperatorFactory) {
      return partitionColumns.equals(((NaivePartitioningOperatorFactory) other).getPartitionColumns());
    }
    return false;
  }

  @Override
  public String toString()
  {
    return "NaivePartitioningOperatorFactory{" +
           "partitionColumns=" + partitionColumns +
           '}';
  }
}
