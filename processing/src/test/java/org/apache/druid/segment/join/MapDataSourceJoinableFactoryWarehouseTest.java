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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class MapDataSourceJoinableFactoryWarehouseTest
{
  @Mock
  private InlineDataSource inlineDataSource;
  @Mock(MockType.NICE)
  private JoinableFactory noopJoinableFactory;
  private NoopDataSource noopDataSource;
  @Mock
  private JoinConditionAnalysis condition;
  @Mock
  private Joinable mockJoinable;

  private MapDataSourceJoinableFactoryWarehouse target;

  @Before
  public void setUp()
  {
    noopDataSource = new NoopDataSource();

    target = new MapDataSourceJoinableFactoryWarehouse(
        ImmutableMap.of(NoopDataSource.class, noopJoinableFactory));
  }
  @Test
  public void testBuildDataSourceNotRegisteredShouldReturnAbsent()
  {
    Optional<Joinable> joinable = target.build(inlineDataSource, condition);
    Assert.assertFalse(joinable.isPresent());
  }

  @Test
  public void testBuildDataSourceIsRegisteredAndFactoryDoesNotBuildJoinableShouldReturnAbsent()
  {
    EasyMock.expect(noopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.empty());
    EasyMock.replay(noopJoinableFactory);
    Optional<Joinable> joinable = target.build(noopDataSource, condition);
    Assert.assertFalse(joinable.isPresent());
  }

  @Test
  public void testBuildDataSourceIsRegisteredShouldReturnJoinableFromFactory()
  {
    EasyMock.expect(noopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.of(mockJoinable));
    EasyMock.replay(noopJoinableFactory);
    Optional<Joinable> joinable = target.build(noopDataSource, condition);
    Assert.assertEquals(mockJoinable, joinable.get());
  }

  /**
   * A datasource that returns nothing. Only used to test un-registered datasources.
   */
  private static class NoopDataSource implements DataSource
  {
    @Override
    public Set<String> getTableNames()
    {
      return null;
    }

    @Override
    public List<DataSource> getChildren()
    {
      return null;
    }

    @Override
    public DataSource withChildren(List<DataSource> children)
    {
      return null;
    }

    @Override
    public boolean isCacheable()
    {
      return false;
    }

    @Override
    public boolean isGlobal()
    {
      return false;
    }

    @Override
    public boolean isConcrete()
    {
      return false;
    }
  }
}
