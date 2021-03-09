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

import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.InlineDataSource;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Optional;

@RunWith(EasyMockRunner.class)
public class MapJoinableFactoryTest
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
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private MapJoinableFactory target;

  @Before
  public void setUp()
  {
    noopDataSource = new NoopDataSource();

    target = new MapJoinableFactory(
        ImmutableSet.of(noopJoinableFactory),
        ImmutableMap.of(noopJoinableFactory.getClass(), NoopDataSource.class)
    );
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
  public void testComputeJoinCacheKey()
  {
    Optional<byte[]> expected = Optional.of(new byte[]{1, 2, 3});
    EasyMock.expect(noopJoinableFactory.computeJoinCacheKey(noopDataSource, condition)).andReturn(expected);
    EasyMock.replay(noopJoinableFactory);
    Optional<byte[]> actual = target.computeJoinCacheKey(noopDataSource, condition);
    Assert.assertSame(expected, actual);
  }

  @Test
  public void testBuildExceptionWhenTwoJoinableFactoryForSameDataSource()
  {
    JoinableFactory anotherNoopJoinableFactory = EasyMock.mock(MapJoinableFactory.class);
    target = new MapJoinableFactory(
        ImmutableSet.of(noopJoinableFactory, anotherNoopJoinableFactory),
        ImmutableMap.of(
            noopJoinableFactory.getClass(),
            NoopDataSource.class,
            anotherNoopJoinableFactory.getClass(),
            NoopDataSource.class
        )
    );
    EasyMock.expect(noopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.of(mockJoinable));
    EasyMock.expect(anotherNoopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.of(mockJoinable));
    EasyMock.replay(noopJoinableFactory, anotherNoopJoinableFactory);
    expectedException.expect(ISE.class);
    expectedException.expectMessage(StringUtils.format(
        "Multiple joinable factories are valid for table[%s]",
        noopDataSource
    ));
    target.build(noopDataSource, condition);
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
