/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

class LookupBean
{
  private final LookupExtractorFactoryContainer container;
  private final String name;

  //to support rollback from 0.10.1 to 0.9.0 if necessary
  @Deprecated
  private final LookupExtractorFactory factory;


  @JsonCreator
  public LookupBean(
      @JsonProperty("name") String name,
      //kept for backward compatibility with druid ver <= 0.10.0 persisted snapshots
      @Deprecated @JsonProperty("factory") LookupExtractorFactory factory,
      @JsonProperty("container") LookupExtractorFactoryContainer container
  )
  {
    Preconditions.checkArgument(factory != null || container != null, "either one of factory or container must exist");

    this.name = name;
    this.container = container != null ? container : new LookupExtractorFactoryContainer(null, factory);
    this.factory = factory != null ? factory : container.getLookupExtractorFactory();
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public LookupExtractorFactoryContainer getContainer()
  {
    return container;
  }

  @Deprecated
  @JsonProperty
  public LookupExtractorFactory getFactory()
  {
    return factory;
  }

  @Override
  public String toString()
  {
    return "LookupBean{" +
           "container=" + container +
           ", name='" + name + '\'' +
           ", factory=" + factory +
           '}';
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
    LookupBean that = (LookupBean) o;
    return Objects.equals(container, that.container) &&
           Objects.equals(name, that.name) &&
           Objects.equals(factory, that.factory);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(container, name, factory);
  }
}
