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

package org.apache.druid.segment.nested;

import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public abstract class StructuredDataProcessor
{
  public static final String ROOT_LITERAL = ".";
  private static final Set<String> ROOT_LITERAL_FIELDS = ImmutableSet.of(ROOT_LITERAL);

  public abstract int processLiteralField(String fieldName, Object fieldValue);

  /**
   * Process fields, returning a list of all "normalized" 'jq' paths to literal fields, consistent with the output of
   * {@link NestedPathFinder#toNormalizedJqPath(List)}.
   *
   * Note: in the future, {@link ProcessResults#getLiteralFields()} should instead probably be modified to deal in
   * lists of {@link NestedPathPart} instead so that callers can decide how to represent the path instead of assuing
   * 'jq' syntax.
   */
  public ProcessResults processFields(Object raw)
  {
    Queue<Field> toProcess = new ArrayDeque<>();
    raw = StructuredData.unwrap(raw);
    if (raw instanceof Map) {
      toProcess.add(new MapField("", (Map<String, ?>) raw));
    } else if (raw instanceof List) {
      toProcess.add(new ListField(ROOT_LITERAL, (List<?>) raw));
    } else {
      return new ProcessResults().withFields(ROOT_LITERAL_FIELDS).withSize(processLiteralField(ROOT_LITERAL, raw));
    }

    ProcessResults accumulator = new ProcessResults();

    while (!toProcess.isEmpty()) {
      Field next = toProcess.poll();
      if (next instanceof MapField) {
        accumulator.merge(processMapField(toProcess, (MapField) next));
      } else if (next instanceof ListField) {
        accumulator.merge(processListField(toProcess, (ListField) next));
      }
    }
    return accumulator;
  }

  private ProcessResults processMapField(Queue<Field> toProcess, MapField map)
  {
    // just guessing a size for a Map as some constant, it might be bigger than this...
    ProcessResults processResults = new ProcessResults().withSize(16);
    for (Map.Entry<String, ?> entry : map.getMap().entrySet()) {
      // add estimated size of string key
      processResults.addSize(estimateStringSize(entry.getKey()));
      final String fieldName = map.getName() + ".\"" + entry.getKey() + "\"";
      Object value = StructuredData.unwrap(entry.getValue());
      // lists and maps go back in the queue
      if (value instanceof List) {
        List<?> theList = (List<?>) value;
        toProcess.add(new ListField(fieldName, theList));
      } else if (value instanceof Map) {
        toProcess.add(new MapField(fieldName, (Map<String, ?>) value));
      } else {
        // literals get processed
        processResults.addLiteralField(fieldName, processLiteralField(fieldName, value));
      }
    }
    return processResults;
  }

  private ProcessResults processListField(Queue<Field> toProcess, ListField list)
  {
    // start with object reference, is probably a bit bigger than this...
    ProcessResults results = new ProcessResults().withSize(8);
    final List<?> theList = list.getList();
    for (int i = 0; i < theList.size(); i++) {
      final String listFieldName = list.getName() + "[" + i + "]";
      final Object element = StructuredData.unwrap(theList.get(i));
      // maps and lists go back into the queue
      if (element instanceof Map) {
        toProcess.add(new MapField(listFieldName, (Map<String, ?>) element));
      } else if (element instanceof List) {
        toProcess.add(new ListField(listFieldName, (List<?>) element));
      } else {
        // literals get processed
        results.addLiteralField(listFieldName, processLiteralField(listFieldName, element));
      }
    }
    return results;
  }

  abstract static class Field
  {
    private final String name;

    protected Field(String name)
    {
      this.name = name;
    }

    public String getName()
    {
      return name;
    }
  }

  static class ListField extends Field
  {
    private final List<?> list;

    ListField(String name, List<?> list)
    {
      super(name);
      this.list = list;
    }

    public List<?> getList()
    {
      return list;
    }
  }

  static class MapField extends Field
  {
    private final Map<String, ?> map;

    MapField(String name, Map<String, ?> map)
    {
      super(name);
      this.map = map;
    }

    public Map<String, ?> getMap()
    {
      return map;
    }
  }

  /**
   * Accumulates the list of literal field paths and a rough size estimation for {@link StructuredDataProcessor}
   */
  public static class ProcessResults
  {
    private Set<String> literalFields;
    private int estimatedSize;

    public ProcessResults()
    {
      literalFields = new HashSet<>();
      estimatedSize = 0;
    }

    public Set<String> getLiteralFields()
    {
      return literalFields;
    }

    public int getEstimatedSize()
    {
      return estimatedSize;
    }

    public ProcessResults addSize(int size)
    {
      this.estimatedSize += size;
      return this;
    }

    public ProcessResults addLiteralField(String fieldName, int sizeOfValue)
    {
      literalFields.add(fieldName);
      this.estimatedSize += sizeOfValue;
      return this;
    }

    public ProcessResults withFields(Set<String> fields)
    {
      this.literalFields = fields;
      return this;
    }

    public ProcessResults withSize(int size)
    {
      this.estimatedSize = size;
      return this;
    }

    public ProcessResults merge(ProcessResults other)
    {
      this.literalFields.addAll(other.literalFields);
      this.estimatedSize += other.estimatedSize;
      return this;
    }
  }

  /**
   * this is copied from {@link org.apache.druid.segment.StringDimensionDictionary#estimateSizeOfValue(String)}
   */
  public static int estimateStringSize(@Nullable String value)
  {
    return value == null ? 0 : 28 + 16 + (2 * value.length());
  }

  public static int getLongObjectEstimateSize()
  {
    // object reference + size of long
    return 8 + Long.BYTES;
  }

  public static int getDoubleObjectEstimateSize()
  {
    // object reference + size of double
    return 8 + Double.BYTES;
  }
}
