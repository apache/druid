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

package org.apache.druid.server;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Tests that the subquery limit utils gives out valid byte limits when supplied with 'auto'.
 * Assuming that the average segment size is 500 MB to 1 GB, and optimal number of rows in the segment is 5 million, the
 * typical bytes per row can be from 100 bytes to 200 bytes. We can keep this in mind while trying to look through the test
 * values
 */
public class SubqueryGuardrailHelperTest
{

  @Test
  public void testConvertSubqueryLimitStringToLongWithoutLookups()
  {
    Assert.assertEquals(
        10737418L,
        fetchSubqueryLimitUtilsForNoLookups(humanReadableSizeToBytes("512MiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        13421772L,
        fetchSubqueryLimitUtilsForNoLookups(humanReadableSizeToBytes("640MiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        16106127L,
        fetchSubqueryLimitUtilsForNoLookups(humanReadableSizeToBytes("768MiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        21474836L,
        fetchSubqueryLimitUtilsForNoLookups(humanReadableSizeToBytes("1GiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        171798691L,
        fetchSubqueryLimitUtilsForNoLookups(humanReadableSizeToBytes("8GiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        429496729L,
        fetchSubqueryLimitUtilsForNoLookups(humanReadableSizeToBytes("20GiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );
  }

  @Test
  public void testConvertSubqueryLimitStringToLongWithLookups()
  {
    Assert.assertEquals(
        10527703L,
        fetchSubqueryLimitUtilsForLookups(humanReadableSizeToBytes("512MiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        13212057L,
        fetchSubqueryLimitUtilsForLookups(humanReadableSizeToBytes("640MiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        15896412,
        fetchSubqueryLimitUtilsForLookups(humanReadableSizeToBytes("768MiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        21265121L,
        fetchSubqueryLimitUtilsForLookups(humanReadableSizeToBytes("1GiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        171588976L,
        fetchSubqueryLimitUtilsForLookups(humanReadableSizeToBytes("8GiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );

    Assert.assertEquals(
        429287014L,
        fetchSubqueryLimitUtilsForLookups(humanReadableSizeToBytes("20GiB"), 25)
            .convertSubqueryLimitStringToLong("auto")
    );
  }


  private SubqueryGuardrailHelper fetchSubqueryLimitUtilsForNoLookups(long maxMemoryInJvm, int brokerNumHttpConnections)
  {
    return new SubqueryGuardrailHelper(null, maxMemoryInJvm, brokerNumHttpConnections);
  }

  private SubqueryGuardrailHelper fetchSubqueryLimitUtilsForLookups(long maxMemoryInJvm, int brokerNumHttpConnections)
  {
    return new SubqueryGuardrailHelper(lookupManager(), maxMemoryInJvm, brokerNumHttpConnections);
  }


  private static long humanReadableSizeToBytes(String humanReadableSize)
  {
    return new HumanReadableBytes(humanReadableSize).getBytes();
  }

  public static LookupExtractorFactoryContainerProvider lookupManager()
  {
    LookupExtractorFactoryContainerProvider lookupManager = EasyMock.mock(LookupExtractorFactoryContainerProvider.class);

    EasyMock.expect(lookupManager.getAllLookupNames()).andReturn(ImmutableSet.of("lookupFoo", "lookupBar")).anyTimes();

    LookupExtractorFactoryContainer lookupFooContainer = EasyMock.mock(LookupExtractorFactoryContainer.class);
    EasyMock.expect(lookupManager.get("lookupFoo")).andReturn(Optional.of(lookupFooContainer));
    LookupExtractorFactory lookupFooExtractorFactory = EasyMock.mock(LookupExtractorFactory.class);
    EasyMock.expect(lookupFooContainer.getLookupExtractorFactory()).andReturn(lookupFooExtractorFactory);
    LookupExtractor lookupFooExtractor = EasyMock.mock(LookupExtractor.class);
    EasyMock.expect(lookupFooExtractorFactory.get()).andReturn(lookupFooExtractor);
    EasyMock.expect(lookupFooExtractor.estimateHeapFootprint()).andReturn(humanReadableSizeToBytes("10MiB"));

    EasyMock.expect(lookupManager.get("lookupBar")).andReturn(Optional.empty());

    EasyMock.replay(lookupManager, lookupFooContainer, lookupFooExtractor, lookupFooExtractorFactory);

    return lookupManager;
  }
}
