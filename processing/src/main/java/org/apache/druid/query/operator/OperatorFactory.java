package org.apache.druid.query.operator;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "naivePartition", value = NaivePartitioningOperatorFactory.class),
    @JsonSubTypes.Type(name = "window", value = WindowOperatorFactory.class),
})
public interface OperatorFactory
{
  Operator wrap(Operator op);
}
