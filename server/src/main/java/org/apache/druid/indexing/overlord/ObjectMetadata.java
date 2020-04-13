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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class ObjectMetadata implements DataSourceMetadata
{
  private final Object theObject;

  @JsonCreator
  public ObjectMetadata(
      @JsonProperty("object") Object theObject
  )
  {
    this.theObject = theObject;
  }

  @JsonProperty("object")
  public Object getObject()
  {
    return theObject;
  }

  @Override
  public boolean isValidStart()
  {
    return theObject == null;
  }

  @Override
  public DataSourceMetadata asStartMetadata()
  {
    return this;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    return equals(other);
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    return other;
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    return this;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof ObjectMetadata) {
      final Object other = ((ObjectMetadata) o).getObject();
      return (theObject == null && other == null) || (theObject != null && theObject.equals(other));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(theObject);
  }

  @Override
  public String toString()
  {
    return "ObjectMetadata{" +
           "theObject=" + theObject +
           '}';
  }
}
