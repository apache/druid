/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package druid.examples.webStream;

import com.google.common.io.InputSupplier;
import com.metamx.druid.jackson.DefaultObjectMapper;
import junit.framework.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WebFirehoseFactoryTest
{
  private final ArrayList<String> dimensions = new ArrayList<String>();
  private InputSupplier testCaseSupplier;
  final int QUEUE_SIZE = 2000;
  BlockingQueue<Map<String, Object>> queue;
  String timeDimension = "t";
  DefaultObjectMapper mapper = new DefaultObjectMapper();
  Map<String, Object> expectedAnswer = new HashMap<String, Object>();

  @BeforeClass
  public void setUp()
  {
    testCaseSupplier = new TestCaseSupplier(
        "{ \"a\": \"Mozilla\\/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko\\/20100101 Firefox\\/21.0\", \"c\": \"US\", \"nk\": 1, \"tz\": \"America\\/New_York\", \"gr\": \"NY\", \"g\": \"1Chgyj\", \"h\": \"15vMQjX\", \"l\": \"o_d63rn9enb\", \"al\": \"en-US,en;q=0.5\", \"hh\": \"1.usa.gov\", \"r\": \"http:\\/\\/forecast.weather.gov\\/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX\", \"u\": \"http:\\/\\/www.spc.ncep.noaa.gov\\/\", \"t\": 1372121562, \"hc\": 1368193091, \"cy\": \"New York\", \"ll\": [ 40.862598, -73.921799 ] }"
    );

    dimensions.add("g");
    dimensions.add("c");
    dimensions.add("a");
    dimensions.add("cy");
    dimensions.add("l");
    dimensions.add("hh");
    dimensions.add("hc");
    dimensions.add("h");
    dimensions.add("u");
    dimensions.add("tz");
    dimensions.add("t");
    dimensions.add("r");
    dimensions.add("gr");
    dimensions.add("nk");
    dimensions.add("al");
    dimensions.add("ll");

    expectedAnswer.put("a", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0");
    expectedAnswer.put("c", "US");
    expectedAnswer.put("nk", 1);
    expectedAnswer.put("tz", "America/New_York");
    expectedAnswer.put("gr", "NY");
    expectedAnswer.put("g", "1Chgyj");
    expectedAnswer.put("h", "15vMQjX");
    expectedAnswer.put("l", "o_d63rn9enb");
    expectedAnswer.put("al", "en-US,en;q=0.5");
    expectedAnswer.put("hh", "1.usa.gov");
    expectedAnswer.put(
        "r",
        "http://forecast.weather.gov/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX"
    );
    expectedAnswer.put("u", "http://www.spc.ncep.noaa.gov/");
    expectedAnswer.put("t", 1372121562);
    expectedAnswer.put("hc", 1368193091);
    expectedAnswer.put("cy", "New York");
    expectedAnswer.put("ll", Arrays.asList(40.862598, -73.921799));

  }

  @Test(expectedExceptions = UnknownHostException.class)
  public void checkInvalidUrl() throws Exception
  {

    String invalidURL = "http://invalid.url";
    WebJsonSupplier supplier = new WebJsonSupplier(invalidURL);
    supplier.getInput();
  }

  @Test
  public void basicIngestionCheck() throws Exception
  {
    queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        queue,
        mapper,
        null,
        timeDimension
    );
    updateStream.run();
    Map<String, Object> insertedRow = queue.poll(10, TimeUnit.SECONDS);
    Assert.assertEquals(expectedAnswer, insertedRow);
  }

  //If a timestamp is missing, we should throw away the event
  @Test
  public void missingTimeStampCheck()
  {
    queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
    InputSupplier testCaseSupplier = new TestCaseSupplier(
        "{ \"a\": \"Mozilla\\/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko\\/20100101 Firefox\\/21.0\", \"c\": \"US\", \"nk\": 1, \"tz\": \"America\\/New_York\", \"gr\": \"NY\", \"g\": \"1Chgyj\", \"h\": \"15vMQjX\", \"l\": \"o_d63rn9enb\", \"al\": \"en-US,en;q=0.5\", \"hh\": \"1.usa.gov\", \"r\": \"http:\\/\\/forecast.weather.gov\\/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX\", \"u\": \"http:\\/\\/www.spc.ncep.noaa.gov\\/\", \"hc\": 1368193091, \"cy\": \"New York\", \"ll\": [ 40.862598, -73.921799 ] }"
    );
    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        queue,
        mapper,
        null,
        timeDimension
    );
    updateStream.run();
    Assert.assertEquals(queue.size(), 0);
  }

  //If any other value is missing, we should still add the event and process it properly
  @Test
  public void otherNullValueCheck() throws Exception
  {
    Map<String, Object> expectedAnswer = new HashMap<String, Object>();
    expectedAnswer.put("a", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0");
    expectedAnswer.put("nk", 1);
    expectedAnswer.put("tz", "America/New_York");
    expectedAnswer.put("gr", "NY");
    expectedAnswer.put("g", "1Chgyj");
    expectedAnswer.put("h", "15vMQjX");
    expectedAnswer.put("l", "o_d63rn9enb");
    expectedAnswer.put("al", "en-US,en;q=0.5");
    expectedAnswer.put("hh", "1.usa.gov");
    expectedAnswer.put(
        "r",
        "http://forecast.weather.gov/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX"
    );
    expectedAnswer.put("u", "http://www.spc.ncep.noaa.gov/");
    expectedAnswer.put("t", 1372121562);
    expectedAnswer.put("hc", 1368193091);
    expectedAnswer.put("cy", "New York");
    expectedAnswer.put("ll", Arrays.asList(40.862598, -73.921799));
    queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
    testCaseSupplier = new TestCaseSupplier(
        "{ \"a\": \"Mozilla\\/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko\\/20100101 Firefox\\/21.0\", \"nk\": 1, \"tz\": \"America\\/New_York\", \"gr\": \"NY\", \"g\": \"1Chgyj\", \"h\": \"15vMQjX\", \"l\": \"o_d63rn9enb\", \"al\": \"en-US,en;q=0.5\", \"hh\": \"1.usa.gov\", \"r\": \"http:\\/\\/forecast.weather.gov\\/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX\", \"u\": \"http:\\/\\/www.spc.ncep.noaa.gov\\/\", \"t\": 1372121562, \"hc\": 1368193091, \"cy\": \"New York\", \"ll\": [ 40.862598, -73.921799 ] }"
    );
    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        queue,
        mapper,
        null,
        timeDimension
    );
    updateStream.run();
    Map<String, Object> insertedRow = queue.poll(10, TimeUnit.SECONDS);
    Assert.assertEquals(expectedAnswer, insertedRow);
  }

  @Test
  public void checkRenameKeys() throws Exception
  {
    Map<String, Object> expectedAnswer = new HashMap<String, Object>();
    queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
    Map<String,String> renamedDimensions = new HashMap<String,String>();
    renamedDimensions.put("g","bitly_hash");
    renamedDimensions.put("c","country");
    renamedDimensions.put("a","user");
    renamedDimensions.put("cy","city");
    renamedDimensions.put("l","encoding_user_login");
    renamedDimensions.put("hh","short_url");
    renamedDimensions.put("hc","timestamp_hash");
    renamedDimensions.put("h","user_bitly_hash");
    renamedDimensions.put("u","url");
    renamedDimensions.put("tz","timezone");
    renamedDimensions.put("t","time");
    renamedDimensions.put("r","referring_url");
    renamedDimensions.put("gr","geo_region");
    renamedDimensions.put("nk","known_users");
    renamedDimensions.put("al","accept_language");
    renamedDimensions.put("ll","latitude_longitude");

    expectedAnswer.put("user", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0");
    expectedAnswer.put("country", "US");
    expectedAnswer.put("known_users", 1);
    expectedAnswer.put("timezone", "America/New_York");
    expectedAnswer.put("geo_region", "NY");
    expectedAnswer.put("bitly_hash", "1Chgyj");
    expectedAnswer.put("user_bitly_hash", "15vMQjX");
    expectedAnswer.put("encoding_user_login", "o_d63rn9enb");
    expectedAnswer.put("accept_language", "en-US,en;q=0.5");
    expectedAnswer.put("short_url", "1.usa.gov");
    expectedAnswer.put(
        "referring_url",
        "http://forecast.weather.gov/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX"
    );
    expectedAnswer.put("url", "http://www.spc.ncep.noaa.gov/");
    expectedAnswer.put("time", 1372121562);
    expectedAnswer.put("timestamp_hash", 1368193091);
    expectedAnswer.put("city", "New York");
    expectedAnswer.put("latitude_longitude", Arrays.asList(40.862598, -73.921799));

    UpdateStream updateStream = new UpdateStream(
        testCaseSupplier,
        queue,
        mapper,
        renamedDimensions,
        timeDimension
    );
    updateStream.run();
    Map<String, Object> inputRow = queue.poll(10, TimeUnit.SECONDS);
    Assert.assertEquals(expectedAnswer, inputRow);
  }

}
