/**
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
package io.druid.data.input.avro;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.google.common.collect.Maps;

/**
 * Helper class to parse and validate the schema mappings provided to the parser
 */
public class AvroSchemaMappingHelper
{
  private static final String delims = ".[]()";
  
  public static Map<String, List<PathComponent>> buildMappingCache(Map<String, String> schemaMappings) 
  {
    Map<String, List<PathComponent>> mappingCache = Maps.newHashMap();
    
    // parse, validate, and insert schema mappings into the cache
    for (Map.Entry<String, String> entry: schemaMappings.entrySet()) {
      mappingCache.put(entry.getKey(), AvroSchemaMappingHelper.parseSchemaMapping(entry.getValue()));
    }
    
    return mappingCache;
  }

  /**
   * Parse and validate the specified schema mapping. Paths are defined using the syntax from <a href=
   * "http://commons.apache.org/proper/commons-beanutils/javadocs/v1.9.2/apidocs/org/apache/commons/beanutils/package-summary.html#standard.basic">
   * commons-beansutil</a>. Period (.) is used to separate fieldnames, square brackets ([]) to surround array
   * indexes, parenthesis to indicate maps. Complex paths chain these together to indicate how to traverse the Avro
   * GenericRecord structure.
   * <p>
   * The language for schema mappings is:
   * 
   * <pre>
   *  MAPPING : fieldName ( SEGMENT )*
   *  SEGMENT : '.' fieldName
   *          | '(' mapKey ')'
   *          | '[' arrayIndex ']'
   * </pre>
   * <p>
   * So each of the following are valid mappings:
   * <ul>
   * <li><code>A</code> - selects the field in the toplevel record
   * <li><code>A.B</code> - selects the subfield B from field A
   * <li><code>A(foo)</code> - selects the map value denoted by key foo from the field A in the top level record.
   * <li><code>A[1]</code> - selects the 2nd entry in the array from field A in the top level record
   * <li><code>A.B[2](hello).D</code> - A complex path! Selects field A, then subfield B, which is an array. Takes
   * the third entry in the array, which is a map, and finds the value denoted by key hello. This in turn is
   * another record, so takes field D.
   * </ul>
   * 
   * @param schemaMapping
   *          String representation of a path through an avro structure
   * @return a list of {@link PathComponent} represent the path through the avro structure
   */
  public static List<PathComponent> parseSchemaMapping(String schemaMapping)
  {
    List<PathComponent> path = new LinkedList<PathComponent>();
    StringTokenizer tok = new StringTokenizer(schemaMapping, delims, true);
    while (tok.hasMoreTokens()) {
      String part = tok.nextToken();
      if (".".equals(part)) {
        continue;
      } else if ("[".equals(part)) {
        String indexStr = tok.nextToken();
        int index = Integer.parseInt(indexStr);
        if (!"]".equals(tok.nextToken())) {
          throw new IllegalArgumentException("Illegal array syntax in schema mapping value: " + schemaMapping);
        }
        path.add(new PathComponent(PathComponent.PathComponentType.ARRAY, index));
      } else if ("(".equals(part)) {
        String keyStr = tok.nextToken();
        if (!")".equals(tok.nextToken())) {
          throw new IllegalArgumentException("Illegal map syntax in schema mapping value: " + schemaMapping);
        }
        path.add(new PathComponent(PathComponent.PathComponentType.MAP, keyStr));
      } else {
        path.add(new PathComponent(PathComponent.PathComponentType.FIELD, part));
      }
    }

    return path;
  }

  /**
   * Used to construct a PathComponent from a single field name. Used when no mapping has been specified 
   * for a field access.
   * @param fieldName The field to access
   * @return The PathComponent representing the field to be accessed.
   */
  public static List<PathComponent> getSimpleFieldAccessor(String fieldName)
  {
    return Collections.singletonList(new PathComponent(PathComponent.PathComponentType.FIELD, fieldName));
  }
}
