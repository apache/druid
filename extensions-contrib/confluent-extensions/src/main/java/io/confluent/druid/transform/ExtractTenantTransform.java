/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.druid.transform;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.Transform;

public class ExtractTenantTransform implements Transform
{
  private final String fieldName;
  private final String name;

  public ExtractTenantTransform(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    this.fieldName = fieldName;
    this.name = name;
  }

  @Override
  public String getName()
  {
    return name;
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
      return value == null ? null : TenantUtils.extractTenant(value.toString());
    };
  }
}
