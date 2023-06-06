/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

import java.util.Objects;

public class ExtractTenantTopicTransform implements Transform
{
  private final String fieldName;
  private final String name;

  public ExtractTenantTopicTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public RowFunction getRowFunction()
  {
    return row -> {
      Object existing = row.getRaw(name);
      // do not overwrite existing values if present
      if (existing != null) {
        return existing;
      }

      Object value = row.getRaw(fieldName);
      return value == null ? null : TenantUtils.extractTenantTopic(value.toString());
    };
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExtractTenantTopicTransform)) {
      return false;
    }
    ExtractTenantTopicTransform that = (ExtractTenantTopicTransform) o;
    return fieldName.equals(that.fieldName) &&
           name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, name);
  }

  @Override
  public String toString()
  {
    return "ExtractTenantTopicTransform{" +
           "fieldName='" + fieldName + '\'' +
           ", name='" + name + '\'' +
           '}';
  }
}
