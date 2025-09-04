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

package org.apache.druid.msq.exec;

import org.apache.druid.msq.querykit.InputNumberDataSource;
import org.apache.druid.msq.querykit.RestrictedInputNumberDataSource;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataServerQueryHandlerUtilsTest
{
  @Test
  public void testTransformDatasource()
  {
    DataSource ds = DataServerQueryHandlerUtils.transformDatasource(new InputNumberDataSource(1), 1, "foo");
    Assertions.assertEquals(ds, TableDataSource.create("foo"));
  }

  @Test
  public void testTransformRestrictedDatasource()
  {
    DataSource ds = DataServerQueryHandlerUtils.transformDatasource(
        new RestrictedInputNumberDataSource(
            1,
            NoRestrictionPolicy.instance()
        ),
        1,
        "foo"
    );
    Assertions.assertEquals(ds, RestrictedDataSource.create(TableDataSource.create("foo"), NoRestrictionPolicy.instance()));
  }
}
