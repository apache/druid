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

package org.apache.druid.java.util.common.parsers;

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ObjectFlatteners
{
  private ObjectFlatteners()
  {
    // No instantiation.
  }

  public static <T> ObjectFlattener<T> create(
      final JSONPathSpec flattenSpec,
      final FlattenerMaker<T> flattenerMaker
  )
  {
    final Map<String, Function<T, Object>> extractors = new LinkedHashMap<>();

    for (final JSONPathFieldSpec fieldSpec : flattenSpec.getFields()) {
      final Function<T, Object> extractor;

      switch (fieldSpec.getType()) {
        case ROOT:
          extractor = obj -> flattenerMaker.getRootField(obj, fieldSpec.getExpr());
          break;
        case PATH:
          extractor = flattenerMaker.makeJsonPathExtractor(fieldSpec.getExpr());
          break;
        case JQ:
          extractor = flattenerMaker.makeJsonQueryExtractor(fieldSpec.getExpr());
          break;
        default:
          throw new UOE("Unsupported field type[%s]", fieldSpec.getType());
      }

      if (extractors.put(fieldSpec.getName(), extractor) != null) {
        throw new IAE("Cannot have duplicate field definition: %s", fieldSpec.getName());
      }
    }

    return new ObjectFlattener<T>()
    {
      @Override
      public Map<String, Object> flatten(final T obj)
      {
        return new AbstractMap<String, Object>()
        {
          @Override
          public int size()
          {
            return keySet().size();
          }

          @Override
          public boolean isEmpty()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean containsKey(final Object key)
          {
            if (key == null) {
              return false;
            }

            return keySet().contains(key.toString());
          }

          @Override
          public boolean containsValue(final Object value)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object get(final Object key)
          {
            final String keyString = key.toString();
            final Function<T, Object> extractor = extractors.get(keyString);
            if (extractor != null) {
              return extractor.apply(obj);
            } else {
              return flattenerMaker.getRootField(obj, keyString);
            }
          }

          @Override
          public Object put(final String key, final Object value)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Object remove(final Object key)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void putAll(final Map<? extends String, ?> m)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void clear()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Set<String> keySet()
          {
            if (flattenSpec.isUseFieldDiscovery()) {
              final Iterable<String> rootFields = flattenerMaker.discoverRootFields(obj);
              if (extractors.isEmpty() && rootFields instanceof Set) {
                return (Set<String>) rootFields;
              } else {
                final Set<String> keys = new LinkedHashSet<>(extractors.keySet());
                Iterables.addAll(keys, rootFields);
                return keys;
              }
            } else {
              return extractors.keySet();
            }
          }

          @Override
          public Collection<Object> values()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public Set<Entry<String, Object>> entrySet()
          {
            return keySet().stream()
                           .map(
                               field -> {
                                 return new Entry<String, Object>()
                                 {
                                   @Override
                                   public String getKey()
                                   {
                                     return field;
                                   }

                                   @Override
                                   public Object getValue()
                                   {
                                     return get(field);
                                   }

                                   @Override
                                   public Object setValue(final Object value)
                                   {
                                     throw new UnsupportedOperationException();
                                   }
                                 };
                               }
                           )
                           .collect(Collectors.toCollection(LinkedHashSet::new));
          }
        };
      }
    };
  }

  public interface FlattenerMaker<T>
  {
    Iterable<String> discoverRootFields(T obj);

    Object getRootField(T obj, String key);

    Function<T, Object> makeJsonPathExtractor(String expr);

    Function<T, Object> makeJsonQueryExtractor(String expr);
  }
}
