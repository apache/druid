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

import java.util.Map;
import java.util.Optional;

@RunWith(EasyMockRunner.class)
public class MapJoinableFactoryTest
{
  /**
   * A utility to create a {@link MapJoinableFactory} to be used by tests.
   */
  public static MapJoinableFactory fromMap(Map<Class<? extends DataSource>, JoinableFactory> map)
  {
    return new MapJoinableFactory(map);
  }

  @Mock
  private InlineDataSource inlineDataSource;
  @Mock(MockType.NICE)
  private JoinableFactory noopJoinableFactory;
  private NoopDataSource noopDataSource;
  @Mock
  private JoinConditionAnalysis condition;
  @Mock
  private Joinable mockJoinable;

  private MapJoinableFactory target;

  @Before
  public void setUp()
  {
    noopDataSource = new NoopDataSource();

    target = new MapJoinableFactory(
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

  @Test
  public void testIsDirectShouldBeFalseForNotRegistered()
  {
    Assert.assertFalse(target.isDirectlyJoinable(inlineDataSource));
  }

  @Test
  public void testIsDirectlyJoinableShouldBeTrueForRegisteredThatIsJoinable()
  {
    EasyMock.expect(noopJoinableFactory.isDirectlyJoinable(noopDataSource)).andReturn(true).anyTimes();
    EasyMock.replay(noopJoinableFactory);
    Assert.assertTrue(target.isDirectlyJoinable(noopDataSource));
  }
}
