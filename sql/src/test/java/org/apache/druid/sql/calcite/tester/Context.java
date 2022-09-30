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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The (query) context test case section.
 */
public class Context extends TestElement
{
  protected final Map<String, Object> context;

  protected Context(Map<String, Object> context)
  {
    this(context, false);
  }

  protected Context(Map<String, Object> context, boolean copy)
  {
    super(ElementType.CONTEXT.sectionName(), copy);
    this.context = context;
  }

  @Override
  public ElementType type()
  {
    return TestElement.ElementType.CONTEXT;
  }

  @Override
  public TestElement copy()
  {
    return new Context(context, true);
  }

  public Map<String, Object> context()
  {
    return context;
  }

  public List<String> sorted()
  {
    List<String> keys = new ArrayList<>(context.keySet());
    Collections.sort(keys);
    List<String> sorted = new ArrayList<>();
    for (String key : keys) {
      sorted.add(key + "=" + context.get(key));
    }
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
    Context other = (Context) o;
    return context.equals(other.context);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(context);
  }

  @Override
  public void writeElement(TestCaseWriter writer) throws IOException
  {
    writer.emitContext(context);
  }
}
