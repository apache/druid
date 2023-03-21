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

package org.apache.druid.query.operator.join;

import org.apache.druid.query.operator.Operator;

import java.util.Arrays;
import java.util.List;

public class JoinPartDefn
{
  public static Builder builder(Operator op)
  {
    return new Builder(op);
  }


  private final Operator op;
  private final List<String> joinFields;
  private final List<String> projectFields;

  public JoinPartDefn(
      Operator op,
      List<String> joinFields,
      List<String> projectFields
  )
  {
    this.op = op;
    this.joinFields = joinFields;
    this.projectFields = projectFields;
  }

  public Operator getOp()
  {
    return op;
  }

  public List<String> getJoinFields()
  {
    return joinFields;
  }

  public List<String> getProjectFields()
  {
    return projectFields;
  }

  public static class Builder
  {
    private final Operator op;
    private List<String> joinFields;
    private List<String> projectFields;

    public Builder(Operator op)
    {
      this.op = op;
    }

    public Builder joinOn(String... fields)
    {
      joinFields = Arrays.asList(fields);
      return this;
    }

    public Builder project(String... fields)
    {
      projectFields = Arrays.asList(fields);
      return this;
    }

    public JoinPartDefn build()
    {
      return new JoinPartDefn(op, joinFields, projectFields);
    }
  }
}
