/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class MetadataSegmentManagerTest
{
  private SQLMetadataSegmentManager manager;
  private DBI dbi;
  private List<DataSegment> testRows;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Before
  public void setUp() throws Exception
  {
    dbi = EasyMock.createMock(DBI.class);
    final Supplier<MetadataStorageTablesConfig> dbTables = Suppliers.ofInstance(MetadataStorageTablesConfig.fromBase("test"));
    manager = new SQLMetadataSegmentManager(
        new DefaultObjectMapper(),
        Suppliers.ofInstance(new MetadataSegmentManagerConfig()),
        dbTables,
        new SQLMetadataConnector(Suppliers.ofInstance(new MetadataStorageConnectorConfig()), dbTables)
        {
          @Override
          protected String getSerialType()
          {
            return null;
          }

          @Override
          protected boolean tableExists(Handle handle, String tableName)
          {
            return false;
          }

          @Override
          public DBI getDBI()
          {
            return dbi;
          }
        }
    );

    DataSegment segment1 = jsonMapper.readValue(
        "{\"dataSource\":\"wikipedia_editstream\",\"interval\":"
        + "\"2012-03-15T00:00:00.000/2012-03-16T00:00:00.000\",\"version\":\"2012-03-16T00:36:30.848Z\""
        + ",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":\"metamx-kafka-data\",\"key\":"
        + "\"wikipedia-editstream/v3/beta-index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index"
        + ".zip\"},\"dimensions\":\"page,namespace,language,user,anonymous,robot,newPage,unpatrolled,"
        + "geo,continent_code,country_name,city,region_lookup,dma_code,area_code,network,postal_code\""
        + ",\"metrics\":\"count,delta,variation,added,deleted\",\"shardSpec\":{\"type\":\"none\"},"
        + "\"size\":26355195,\"identifier\":\"wikipedia_editstream_2012-03-15T00:00:00.000Z_2012-03-16"
        + "T00:00:00.000Z_2012-03-16T00:36:30.848Z\"}",
        DataSegment.class
    );

    DataSegment segment2 = jsonMapper.readValue(
        "{\"dataSource\":\"twitterstream\",\"interval\":\"2012-01-05T00:00:00.000/2012-01-06T00:00:00.000\","
        + "\"version\":\"2012-01-06T22:19:12.565Z\",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":"
        + "\"metamx-twitterstream\",\"key\":\"index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip\"}"
        + ",\"dimensions\":\"user_name,user_lang,user_time_zone,user_location,"
        + "user_mention_name,has_mention,reply_to_name,first_hashtag,rt_name,url_domain,has_links,"
        + "has_geo,is_retweet,is_viral\",\"metrics\":\"count,tweet_length,num_followers,num_links,"
        + "num_mentions,num_hashtags,num_favorites,user_total_tweets\",\"shardSpec\":{\"type\":\"none\"},"
        + "\"size\":511804455,\"identifier\":"
        + "\"twitterstream_2012-01-05T00:00:00.000Z_2012-01-06T00:00:00.000Z_2012-01-06T22:19:12.565Z\"}",
        DataSegment.class
    );

    testRows = Arrays.<DataSegment>asList(segment1, segment2);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(dbi);
  }

  @Test
  public void testPoll()
  {
    EasyMock.expect(dbi.withHandle(EasyMock.<HandleCallback>anyObject())).andReturn(testRows).times(2);
    EasyMock.replay(dbi);

    manager.start();
    manager.poll();
    manager.stop();
  }
}
