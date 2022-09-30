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

package org.apache.druid.sql.calcite.tester;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.ResourceAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The resource (actions) test case section.
 */
public class Resources extends TestElement
{
  /**
   * Indicates an expected resource action.
   */
  public static class Resource
  {
    final String type;
    final String name;
    final Action action;

    public Resource(String type, String name, Action action)
    {
      this.type = type;
      this.name = name;
      this.action = action;
    }

    public Resource(ResourceAction action)
    {
      this(
          action.getResource().getType(),
          action.getResource().getName(),
          action.getAction()
      );
    }

    @Override
    public String toString()
    {
      return type + "/" + name + "/" + action.name();
    }

    public static List<Resource> convert(Set<ResourceAction> actions)
    {
      List<Resource> converted = new ArrayList<>();
      for (ResourceAction action : actions) {
        converted.add(new Resource(action));
      }
      return converted;
    }

    public static List<Resource> sort(List<Resource> list)
    {
      List<Resource> sorted = new ArrayList<>(list);
      Collections.sort(
          sorted,
          (l, r) -> {
            int value = l.type.compareTo(r.type);
            if (value != 0) {
              return value;
            }
            value = l.name.compareTo(r.name);
            if (value != 0) {
              return value;
            }
            return l.action.compareTo(r.action);
          }
      );
      return sorted;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == this) {
        return true;
      }
      if (o == null || o.getClass() != getClass()) {
        return false;
      }
      Resource other = (Resource) o;
      return this.type.equalsIgnoreCase(other.type)
          && this.name.equals(other.name)
          && this.action == other.action;
    }

    /**
     * Never used (doesn't make sense). But, needed to make static checks happy.
     */
    @Override
    public int hashCode()
    {
      return Objects.hash(type, name, action);
    }
  }

  protected final List<Resources.Resource> resourceActions;

  protected Resources(List<Resource> resourceActions)
  {
    this(resourceActions, false);
  }

  protected Resources(List<Resource> resourceActions, boolean copy)
  {
    super(ElementType.RESOURCES.sectionName(), copy);
    this.resourceActions = resourceActions;
  }

  public List<Resources.Resource> resourceActions()
  {
    return resourceActions;
  }

  @Override
  public ElementType type()
  {
    return ElementType.RESOURCES;
  }

  @Override
  public TestElement copy()
  {
    return new Resources(resourceActions, true);
  }

  public boolean verify(Set<ResourceAction> actual, ActualResults.ErrorCollector errors)
  {
    if (actual == null) {
      return true;
    }
    if (actual.size() != resourceActions.size()) {
      errors.setSection(type().sectionName());
      errors.add(
          StringUtils.format(
              "expected %d entries, got %d",
              resourceActions.size(),
              actual.size()));
      return false;
    }
    List<Resources.Resource> expectedActions = Resources.Resource.sort(resourceActions);
    List<Resources.Resource> actualActions = Resources.Resource.sort(Resources.Resource.convert(actual));
    for (int i = 0; i < expectedActions.size(); i++) {
      if (!expectedActions.get(i).equals(actualActions.get(i))) {
        errors.setSection(type().sectionName());
        errors.add(
            StringUtils.format(
                "resource did not match: [%s]",
                actualActions.get(i)));
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    Resources other = (Resources) o;
    return resourceActions.equals(other.resourceActions);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(resourceActions);
  }

  @Override
  public void writeElement(TestCaseWriter writer) throws IOException
  {
    writer.emitResources(resourceActions);
  }
}
