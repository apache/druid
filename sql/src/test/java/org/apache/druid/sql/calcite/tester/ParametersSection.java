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

import org.apache.druid.sql.http.SqlParameter;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * The parameters test case section.
 */
public class ParametersSection extends TestSection
{
  protected final List<SqlParameter> parameters;

  protected ParametersSection(List<SqlParameter> parameters)
  {
    this(parameters, false);
  }

  protected ParametersSection(List<SqlParameter> parameters, boolean copy)
  {
    super(Section.PARAMETERS.sectionName(), copy);
    this.parameters = parameters;
  }

  public List<SqlParameter> parameters()
  {
    return parameters;
  }

  @Override
  public TestSection.Section section()
  {
    return TestSection.Section.PARAMETERS;
  }

  @Override
  public TestSection copy()
  {
    return new ParametersSection(parameters, true);
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
    ParametersSection other = (ParametersSection) o;
    return parameters.equals(other.parameters);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(parameters);
  }

  @Override
  public void writeSection(TestCaseWriter writer) throws IOException
  {
    writer.emitParameters(parameters);
  }
}
