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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public abstract class StructuredDataProcessor
{
  public abstract int processLiteralField(ArrayList<NestedPathPart> fieldPath, Object fieldValue);

  /**
   * Process fields, returning a list of all paths to literal fields, represented as an ordered sequence of
   * {@link NestedPathPart}.
   */
  public ProcessResults processFields(Object raw)
  {
    Queue<Field> toProcess = new ArrayDeque<>();
    raw = StructuredData.unwrap(raw);
    ArrayList<NestedPathPart> newPath = new ArrayList<>();
    if (raw instanceof Map) {
      toProcess.add(new MapField(newPath, (Map<String, ?>) raw));
    } else if (raw instanceof List) {
      toProcess.add(new ListField(newPath, (List<?>) raw));
    } else {
      return new ProcessResults().addLiteralField(newPath, processLiteralField(newPath, raw));
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
      Object value = StructuredData.unwrap(entry.getValue());
      // lists and maps go back in the queue
      final ArrayList<NestedPathPart> newPath = new ArrayList<>(map.getPath());
      newPath.add(new NestedPathField(entry.getKey()));
      if (value instanceof List) {
        List<?> theList = (List<?>) value;
        toProcess.add(new ListField(newPath, theList));
      } else if (value instanceof Map) {
        toProcess.add(new MapField(newPath, (Map<String, ?>) value));
      } else {
        // literals get processed
        processResults.addLiteralField(newPath, processLiteralField(newPath, value));
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
      final ArrayList<NestedPathPart> newPath = new ArrayList<>(list.getPath());
      newPath.add(new NestedPathArrayElement(i));
      final Object element = StructuredData.unwrap(theList.get(i));
      // maps and lists go back into the queue
      if (element instanceof Map) {
        toProcess.add(new MapField(newPath, (Map<String, ?>) element));
      } else if (element instanceof List) {
        toProcess.add(new ListField(newPath, (List<?>) element));
      } else {
        // literals get processed
        results.addLiteralField(newPath, processLiteralField(newPath, element));
      }
    }
    return results;
  }

  abstract static class Field
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

  static class ListField extends Field
  {
    private final List<?> list;

    ListField(ArrayList<NestedPathPart> path, List<?> list)
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
