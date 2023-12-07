/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.impl.systemfield;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.segment.transform.RowFunction;
import org.apache.druid.segment.transform.TransformedInputRow;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Decorator of {@link InputRow} from an {@link InputEntity}.
 */
public class SystemFieldDecoratorFactory
{
  /**
   * Decorator factory that does not generate system fields.
   */
  public static final SystemFieldDecoratorFactory NONE =
      new SystemFieldDecoratorFactory((entity, field) -> null, EnumSet.noneOf(SystemField.class));

  private final BiFunction<InputEntity, SystemField, Object> extractor;
  private final Set<SystemField> fields;

  /**
   * Constructor. Package-private; most callers should use {@link #fromInputSource(SystemFieldInputSource)}
   * or {@link #NONE}.
   */
  SystemFieldDecoratorFactory(
      final BiFunction<InputEntity, SystemField, Object> extractor,
      final Set<SystemField> fields
  )
  {
    this.extractor = extractor;
    this.fields = fields;
  }

  /**
   * Create a decorator factory for a given {@link SystemFieldInputSource}.
   */
  public static SystemFieldDecoratorFactory fromInputSource(final SystemFieldInputSource source)
  {
    return new SystemFieldDecoratorFactory(source::getSystemFieldValue, source.getConfiguredSystemFields());
  }

  /**
   * Create a decorator for the given {@link InputEntity}. All {@link InputRow} for a given {@link InputEntity}
   * have the same value for all {@link SystemField}.
   */
  public Function<InputRow, InputRow> decorator(final InputEntity entity)
  {
    if (fields.isEmpty()) {
      return Function.identity();
    } else {
      final Map<String, RowFunction> transforms = new HashMap<>();

      for (final SystemField field : fields) {
        final Object fieldValue = extractor.apply(entity, field);
        transforms.put(field.toString(), new ConstantRowFunction(fieldValue));
      }

      return row -> new TransformedInputRow(row, transforms);
    }
  }

  /**
   * Row function that returns a constant value. Helper for {@link #decorator(InputEntity)}.
   */
  private static class ConstantRowFunction implements RowFunction
  {
    private final Object value;

    public ConstantRowFunction(Object value)
    {
      this.value = value;
    }

    @Override
    public Object eval(Row row)
    {
      return value;
    }

    @Override
    public List<String> evalDimension(Row row)
    {
      return Collections.singletonList(value == null ? null : String.valueOf(value));
    }
  }
}
