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

/**
 * Class representing a step through a complex avro structure. Each PathComponent represents accessing
 * a field in a record, an entry in a map, or an entry in an array. Access to a specific piece of data
 * nested within a complex avro structure can be represented as an ordered list of Path Components. 
 */
public class PathComponent
{
  public enum PathComponentType
  {
    FIELD, ARRAY, MAP
  }

  private final Object value;
  private final PathComponent.PathComponentType type;

  public PathComponent(PathComponent.PathComponentType type, Object value)
  {
    this.type = type;
    this.value = value;
  }

  public PathComponent.PathComponentType getType()
  {
    return type;
  }

  public Integer getArrayIdx()
  {
    return (Integer) value;
  }

  public String getMapKey()
  {
    return (String) value;
  }

  public String getFieldName()
  {
    return (String) value;
  }
}
