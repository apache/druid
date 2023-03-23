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

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

public abstract class StructuredDataProcessor
{
  protected StructuredDataProcessor()
  {
  }

  /**
   * process a value that is definitely not a {@link Map}, {@link List}, or {@link Object[]}
   */
  public abstract ProcessedValue<?> processField(ArrayList<NestedPathPart> fieldPath, @Nullable Object fieldValue);

  /**
   * Process a {@link List} or {@link Object[]} returning a {@link ProcessedValue} if no further processing should
   * be performed by the {@link StructuredDataProcessor}, else a return value of null indicates that each element
   * of the array will be processed separately as a new {@link NestedPathArrayElement} part.
   */
  @Nullable
  public abstract ProcessedValue<?> processArrayField(ArrayList<NestedPathPart> fieldPath, @Nullable List<?> array);


  /**
   * Process some object, traversing any nested structure and returning a list of all paths which created a
   * {@link ProcessedValue} during processing, represented as an ordered sequence of {@link NestedPathPart}.
   *
   * This method processes plain java objects, for each {@link Map} it adds a {@link MapField} to the path, for
   * {@link List} a {@link ArrayField}, {@link Object[]} a {@link ArrayField}, and so on. {@link ArrayField} and
   * {@link ArrayField} will be processed by {@link #processArrayField(ArrayList, List)}
   */
  public ProcessResults processFields(Object raw)
  {
    final Queue<Field> toProcess = new ArrayDeque<>();
    raw = StructuredData.unwrap(raw);
    final ArrayList<NestedPathPart> newPath = new ArrayList<>();
    if (raw instanceof Map) {
      toProcess.add(new MapField(newPath, (Map<String, ?>) raw));
    } else if (raw instanceof List) {
      toProcess.add(new ArrayField(newPath, (List<?>) raw));
    } else if (raw instanceof Object[]) {
      toProcess.add(new ArrayField(newPath, Arrays.asList((Object[]) raw)));
    } else {
      return new ProcessResults().addLiteralField(newPath, processField(newPath, raw).getSize());
    }

    final ProcessResults accumulator = new ProcessResults();

    while (!toProcess.isEmpty()) {
      Field next = toProcess.poll();
      if (next instanceof MapField) {
        accumulator.merge(processMapField(toProcess, (MapField) next));
      } else if (next instanceof ArrayField) {
        accumulator.merge(processArrayField(toProcess, (ArrayField) next));
      }
    }
    return accumulator;
  }

  private ProcessResults processMapField(Queue<Field> toProcess, MapField map)
  {
    // just guessing a size for a Map as some constant, it might be bigger than this...
    final ProcessResults processResults = new ProcessResults().withSize(16);
    for (Map.Entry<String, ?> entry : map.getMap().entrySet()) {
      // add estimated size of string key
      processResults.addSize(estimateStringSize(entry.getKey()));
      Object value = StructuredData.unwrap(entry.getValue());
      // lists and maps go back in the queue
      final ArrayList<NestedPathPart> newPath = new ArrayList<>(map.getPath());
      newPath.add(new NestedPathField(entry.getKey()));
      if (value instanceof List) {
        List<?> theList = (List<?>) value;
        toProcess.add(new ArrayField(newPath, theList));
      } else if (value instanceof Object[]) {
        toProcess.add(new ArrayField(newPath, Arrays.asList((Object[]) value)));
      } else if (value instanceof Map) {
        toProcess.add(new MapField(newPath, (Map<String, ?>) value));
      } else {
        // literals get processed
        processResults.addLiteralField(newPath, processField(newPath, value).getSize());
      }
    }
    return processResults;
  }

  private ProcessResults processArrayField(Queue<Field> toProcess, ArrayField list)
  {
    // start with object reference, is probably a bit bigger than this...
    final ProcessResults results = new ProcessResults().withSize(8);
    final List<?> theList = list.getList();
    // check to see if the processor handled the array, indicated by a non-null result, if so we can stop here
    final ProcessedValue<?> maybeProcessed = processArrayField(list.getPath(), theList);
    if (maybeProcessed != null) {
      results.addLiteralField(list.getPath(), maybeProcessed.getSize());
    } else {
      // else we have to dig into the list and process each element
      for (int i = 0; i < theList.size(); i++) {
        final ArrayList<NestedPathPart> newPath = new ArrayList<>(list.getPath());
        newPath.add(new NestedPathArrayElement(i));
        final Object element = StructuredData.unwrap(theList.get(i));
        // maps and lists go back into the queue
        if (element instanceof Map) {
          toProcess.add(new MapField(newPath, (Map<String, ?>) element));
        } else if (element instanceof List) {
          toProcess.add(new ArrayField(newPath, (List<?>) element));
        } else if (element instanceof Object[]) {
          toProcess.add(new ArrayField(newPath, Arrays.asList((Object[]) element)));
        } else {
          results.addLiteralField(newPath, processField(newPath, element).getSize());
        }
      }
    }
    return results;
  }

  private abstract static class Field
  {
    private final ArrayList<NestedPathPart> path;

    protected Field(ArrayList<NestedPathPart> path)
    {
      this.path = path;
    }

    public ArrayList<NestedPathPart> getPath()
    {
      return path;
    }
  }

  static class ArrayField extends Field
  {
    private final List<?> list;

    ArrayField(ArrayList<NestedPathPart> path, List<?> list)
    {
      super(path);
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

    MapField(ArrayList<NestedPathPart> path, Map<String, ?> map)
    {
      super(path);
      this.map = map;
    }

    public Map<String, ?> getMap()
    {
      return map;
    }
  }

  public static class ProcessedValue<T>
  {
    public static final ProcessedValue<?> NULL_LITERAL = new ProcessedValue<>(null, 0);
    @Nullable
    private final T value;
    private final int size;

    public ProcessedValue(@Nullable T value, int size)
    {
      this.value = value;
      this.size = size;
    }

    @SuppressWarnings("unused")
    @Nullable
    public T getValue()
    {
      return value;
    }

    public int getSize()
    {
      return size;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProcessedValue<?> that = (ProcessedValue<?>) o;
      return size == that.size && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(value, size);
    }
  }

  /**
   * Accumulates the list of literal field paths and a rough size estimation for {@link StructuredDataProcessor}
   */
  public static class ProcessResults
  {
    private Set<ArrayList<NestedPathPart>> literalFields;
    private int estimatedSize;

    public ProcessResults()
    {
      literalFields = new HashSet<>();
      estimatedSize = 0;
    }

    public Set<ArrayList<NestedPathPart>> getLiteralFields()
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

    public ProcessResults addLiteralField(ArrayList<NestedPathPart> fieldPath, int sizeOfValue)
    {
      literalFields.add(fieldPath);
      this.estimatedSize += sizeOfValue;
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
