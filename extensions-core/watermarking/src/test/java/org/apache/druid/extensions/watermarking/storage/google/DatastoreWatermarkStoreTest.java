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

package org.apache.druid.extensions.watermarking.storage.google;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.datastore.v1.Datastore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.extensions.watermarking.WatermarkKeeperConfig;
import org.apache.druid.extensions.watermarking.watermarks.DataCompletenessLowWatermark;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class DatastoreWatermarkStoreTest
{
  @Test
  public void testSaveGet() throws Exception
  {
    Assume.assumeNotNull(System.getenv("DATASTORE_EMULATOR_HOST"), "Missing DATASTORE_EMULATOR_HOST, skipping test");
    final DatastoreWatermarkStoreConfig config = new DatastoreWatermarkStoreConfig()
    {

      @Override
      public String getProjectId()
      {
        return "test";
      }

      @Override
      public String getAccessKeyPath()
      {
        return null;
      }

      @Override
      public String getNamespace()
      {
        return "somenamespace";
      }
    };
    final HttpTransport httpTransport = new NetHttpTransport();
    final Datastore datastore = new Datastore.Builder(
        httpTransport,
        JacksonFactory.getDefaultInstance(),
        GoogleCredential.getApplicationDefault(httpTransport, JacksonFactory.getDefaultInstance())
    ).setRootUrl(StringUtils.format("http://%s/", System.getenv("DATASTORE_EMULATOR_HOST"))).build();
    final WatermarkKeeperConfig keeperConfig = new WatermarkKeeperConfig()
    {
      @Override
      public int getMaxHistoryResults()
      {
        return Integer.MAX_VALUE;
      }
    };
    final DatastoreWatermarkStore datastoreWatermarkStore = new DatastoreWatermarkStore(
        datastore,
        config,
        keeperConfig
    );
    final String datasource = UUID.randomUUID().toString();
    final Interval interval = Intervals.of("2010/2999");
    Assert.assertEquals(
        0,
        Objects.requireNonNull(
            datastoreWatermarkStore.getValueHistory(
                datasource,
                DataCompletenessLowWatermark.TYPE,
                interval
            )
        ).size()
    );
    final DateTime dt = interval.getStart();
    datastoreWatermarkStore.update(datasource, DataCompletenessLowWatermark.TYPE, dt);
    Assert.assertEquals(
        ImmutableList.of(dt),
        Objects.requireNonNull(
            datastoreWatermarkStore.getValueHistory(
                datasource,
                DataCompletenessLowWatermark.TYPE,
                interval
            )
        ).stream().map(p -> p.lhs).collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableMap.of(
            DataCompletenessLowWatermark.TYPE, dt
        ),
        Objects.requireNonNull(
            datastoreWatermarkStore.getValues(
                datasource
            )
        )
    );
    Assert.assertEquals(
        dt,
        Objects.requireNonNull(
            datastoreWatermarkStore.getValue(
                datasource,
                DataCompletenessLowWatermark.TYPE
            )
        )
    );
    Assert.assertArrayEquals(new String[]{datasource}, datastoreWatermarkStore.getDatasources().toArray());
    datastoreWatermarkStore.purgeHistory(datasource, DataCompletenessLowWatermark.TYPE, DateTimes.nowUtc().plus(10));
    Assert.assertTrue(
        Objects.requireNonNull(
            datastoreWatermarkStore.getValueHistory(
                datasource,
                DataCompletenessLowWatermark.TYPE,
                interval
            )
        ).isEmpty()
    );
    // Testing rollback
    datastoreWatermarkStore.update(datasource, DataCompletenessLowWatermark.TYPE, dt);
    final DateTime rollbackTo = DateTimes.nowUtc();
    Thread.sleep(2); // Make sure the UPDATE timestamp is not the same
    datastoreWatermarkStore.update(datasource, DataCompletenessLowWatermark.TYPE, dt.plus(1));
    Assert.assertEquals(
        dt.plus(1),
        datastoreWatermarkStore.getValue(datasource, DataCompletenessLowWatermark.TYPE)
    );
    Assert.assertNotEquals(
        dt,
        datastoreWatermarkStore.getValue(datasource, DataCompletenessLowWatermark.TYPE)
    );
    datastoreWatermarkStore.rollback(datasource, DataCompletenessLowWatermark.TYPE, rollbackTo);
    Assert.assertEquals(
        dt,
        datastoreWatermarkStore.getValue(datasource, DataCompletenessLowWatermark.TYPE)
    );
    datastoreWatermarkStore.purgeHistory(
        datasource,
        DataCompletenessLowWatermark.TYPE,
        DateTimes.nowUtc().plus(1000000)
    );
    Assert.assertNull(datastoreWatermarkStore.getValue(datasource, DataCompletenessLowWatermark.TYPE));
    Assert.assertNull(datastoreWatermarkStore.getDatasources());
  }
}
