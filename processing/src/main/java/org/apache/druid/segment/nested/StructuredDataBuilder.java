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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StructuredDataBuilder
{

  private final List<Element> elements;

  StructuredDataBuilder(StructuredDataBuilder.Element... elements)
  {
    this(List.of(elements));
  }

  StructuredDataBuilder(List<Element> elements)
  {
    this.elements = elements;
  }

  /**
   * Creates a StructuredDataBuilder from a list of paths and corresponding objects.
   */
  StructuredDataBuilder(List<List<NestedPathPart>> parts, List<Object> objects)
  {
    List<Element> elements = new ArrayList<>();
    for (int i = 0; i < parts.size(); i++) {
      elements.add(Element.of(parts.get(i), objects.get(i)));
    }
    this.elements = elements;
  }

  public StructuredData build()
  {
    Object subtree = buildObject();
    return StructuredData.wrap(subtree == null ? Map.of() : subtree);
  }

  @Nullable
  private Object buildObject()
  {
    Object simpleObject = null;
    Multimap<String, Element> map = LinkedListMultimap.create();
    ArrayList<List<Element>> list = new ArrayList<>();

    for (Element element : elements) {
      if (element.getValue() == null) {
        // we can't distinguish between null and missing values in structured data
        continue;
      }

      if (element.endOfPath()) {
        simpleObject = element.getValue();
        continue;
      }

      NestedPathPart currentPath = element.getCurrentPath();
      if (currentPath instanceof NestedPathField) {
        map.put(((NestedPathField) currentPath).getField(), element.next());
      } else if (currentPath instanceof NestedPathArrayElement) {
        int index = ((NestedPathArrayElement) currentPath).getIndex();
        while (list.size() <= index) {
          list.add(new ArrayList<>());
        }
        list.get(index).add(element.next());
      }
    }

    if (simpleObject != null) {
      if (!(map.isEmpty() && list.isEmpty())) {
        throw DruidException.defensive(
            "Error building structured data from paths[%s], cannot have map or array elements when root value is set",
            elements
        );
      }
      return simpleObject;
    } else if (!map.isEmpty()) {
      if (!list.isEmpty()) {
        throw DruidException.defensive(
            "Error building structured data from paths[%s], cannot have both map and array elements at the same level",
            elements
        );
      }
      return Maps.transformValues(
          map.asMap(),
          (mapElements) -> new StructuredDataBuilder(new ArrayList<>(mapElements)).buildObject()
      );
    } else if (!list.isEmpty()) {
      List<Object> resultList = new ArrayList<>(list.size());
      for (List<Element> elementList : list) {
        resultList.add(new StructuredDataBuilder(elementList).buildObject());
      }
      return resultList;
    }
    return null;
  }

  public static class Element
  {
    final List<NestedPathPart> path;
    @Nullable
    final Object value;
    final int depth;

    Element(List<NestedPathPart> path, Object value, int depth)
    {
      this.path = path;
      this.value = value;
      this.depth = depth;
    }

    static Element of(List<NestedPathPart> path, Object value)
    {
      return new Element(path, value, 0);
    }

    @Nullable
    Object getValue()
    {
      return value;
    }

    NestedPathPart getCurrentPath()
    {
      return path.get(depth);
    }

    boolean endOfPath()
    {
      return path.size() == depth;
    }

    Element next()
    {
      return new Element(path, value, depth + 1);
    }

    @Override
    public String toString()
    {
      return "Element{" +
             "path=" + path +
             ", value=" + value +
             ", depth=" + depth +
             '}';
    }
  }
}
