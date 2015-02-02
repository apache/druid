/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.db;

import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class DatabaseSegmentManagerTest
{
  private DatabaseSegmentManager manager;
  private IDBI dbi;
  private List<Map<String, Object>> testRows;

  @Before
  public void setUp() throws Exception
  {
    dbi = EasyMock.createMock(IDBI.class);
    manager = new DatabaseSegmentManager(
        new DefaultObjectMapper(),
        Suppliers.ofInstance(new DatabaseSegmentManagerConfig()),
        Suppliers.ofInstance(DbTablesConfig.fromBase("test")),
        dbi
    );

    Map<String, Object> map1 = Maps.newHashMap();
    map1.put("id", "wikipedia_editstream_2012-03-15T00:00:00.000Z_2012-03-16T00:00:00.000Z_2012-03-16T00:36:30.848Z");
    map1.put("dataSource", "wikipedia_editstream");
    map1.put("created_date", "2012-03-23T22:27:21.957Z");
    map1.put("start", "2012-03-15T00:00:00.000Z");
    map1.put("end", "2012-03-16T00:00:00.000Z");
    map1.put("partitioned", 0);
    map1.put("version", "2012-03-16T00:36:30.848Z");
    map1.put("used", 1);
    map1.put(
        "payload", "{\"dataSource\":\"wikipedia_editstream\",\"interval\":"
                   + "\"2012-03-15T00:00:00.000/2012-03-16T00:00:00.000\",\"version\":\"2012-03-16T00:36:30.848Z\""
                   + ",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":\"metamx-kafka-data\",\"key\":"
                   + "\"wikipedia-editstream/v3/beta-index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index"
                   + ".zip\"},\"dimensions\":\"page,namespace,language,user,anonymous,robot,newPage,unpatrolled,"
                   + "geo,continent_code,country_name,city,region_lookup,dma_code,area_code,network,postal_code\""
                   + ",\"metrics\":\"count,delta,variation,added,deleted\",\"shardSpec\":{\"type\":\"none\"},"
                   + "\"size\":26355195,\"identifier\":\"wikipedia_editstream_2012-03-15T00:00:00.000Z_2012-03-16"
                   + "T00:00:00.000Z_2012-03-16T00:36:30.848Z\"}"
    );

    Map<String, Object> map2 = Maps.newHashMap();
    map2.put("id", "twitterstream_2012-01-05T00:00:00.000Z_2012-01-06T00:00:00.000Z_2012-01-06T22:19:12.565Z");
    map2.put("dataSource", "twitterstream");
    map2.put("created_date", "2012-03-23T22:27:21.988Z");
    map2.put("start", "2012-01-05T00:00:00.000Z");
    map2.put("end", "2012-01-06T00:00:00.000Z");
    map2.put("partitioned", 0);
    map2.put("version", "2012-01-06T22:19:12.565Z");
    map2.put("used", 1);
    map2.put(
        "payload", "{\"dataSource\":\"twitterstream\",\"interval\":\"2012-01-05T00:00:00.000/2012-01-06T00:00:00.000\","
                   + "\"version\":\"2012-01-06T22:19:12.565Z\",\"loadSpec\":{\"type\":\"s3_zip\",\"bucket\":"
                   + "\"metamx-twitterstream\",\"key\":\"index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip\"}"
                   + ",\"dimensions\":\"user_name,user_lang,user_time_zone,user_location,"
                   + "user_mention_name,has_mention,reply_to_name,first_hashtag,rt_name,url_domain,has_links,"
                   + "has_geo,is_retweet,is_viral\",\"metrics\":\"count,tweet_length,num_followers,num_links,"
                   + "num_mentions,num_hashtags,num_favorites,user_total_tweets\",\"shardSpec\":{\"type\":\"none\"},"
                   + "\"size\":511804455,\"identifier\":"
                   + "\"twitterstream_2012-01-05T00:00:00.000Z_2012-01-06T00:00:00.000Z_2012-01-06T22:19:12.565Z\"}"
    );

    testRows = Arrays.<Map<String, Object>>asList(map1, map2);
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
