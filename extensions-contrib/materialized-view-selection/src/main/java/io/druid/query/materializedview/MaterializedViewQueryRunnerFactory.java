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

package io.druid.query.materializedview;

import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.segment.Segment;

import java.util.concurrent.ExecutorService;

/**
 * Created by zhangxinyu on 2018/2/5.
 */
public class MaterializedViewQueryRunnerFactory implements QueryRunnerFactory 
{
  @Override
  public QueryRunner createRunner(Segment segment) 
  {
    // never used
    // TODO This method may be used in sql in the next version
    return null;
  }

  @Override
  public QueryToolChest getToolchest() 
  {
    // never used
    // TODO This method may be used in sql in the next version
    return null;
  }

  @Override
  public QueryRunner mergeRunners(ExecutorService queryExecutor, Iterable iterable) 
  {
    // never used
    // TODO This method may be used in sql in the next version
    return null;
  }
}
