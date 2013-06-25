package druid.examples.webStream;

import com.google.common.io.InputSupplier;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WebFirehoseFactoryTest
{
  @Test
  void testMalformedUrlConnect() throws Exception
  {
    List<String> dimensions = new LinkedList<String>();
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

    String invalidURL = "http://invalid.url";
    FirehoseFactory test = new WebFirehoseFactory(invalidURL,dimensions,dimensions,"t");
    Firehose returnedFirehose = test.connect();
    Thread.sleep(3000);
    assert returnedFirehose.hasMore() == false;
  }


  @Test
  public void testUrlWithNoJsonStreamConnect() throws Exception
  {
    List<String> dimensions = new LinkedList<String>();
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

    String nonJsonUrl = "http://google.com";
    FirehoseFactory test = new WebFirehoseFactory(nonJsonUrl,dimensions,dimensions,"t");
    Firehose returnedFirehose = test.connect();
    Thread.sleep(3000);
    assert returnedFirehose.hasMore()== false;
  }

  @Test
  public void correctUrlCheck() throws Exception
  {
    List<String> dimensions = new LinkedList<String>();
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

    String url = "http://developer.usa.gov/1usagov";
    FirehoseFactory test = new WebFirehoseFactory(url,dimensions,dimensions,"t");
    Firehose returnedFirehose = test.connect();
    Thread.sleep(3000);
    assert returnedFirehose.hasMore()== true;
  }


  @Test
  public void basicIngestionCheck() throws Exception
  {
    final int QUEUE_SIZE=2000;
    BlockingQueue<Map<String,Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
    InputSupplier testCaseSupplier = new TestCaseSupplier("{ \"a\": \"Mozilla\\/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko\\/20100101 Firefox\\/21.0\", \"c\": \"US\", \"nk\": 1, \"tz\": \"America\\/New_York\", \"gr\": \"NY\", \"g\": \"1Chgyj\", \"h\": \"15vMQjX\", \"l\": \"o_d63rn9enb\", \"al\": \"en-US,en;q=0.5\", \"hh\": \"1.usa.gov\", \"r\": \"http:\\/\\/forecast.weather.gov\\/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX\", \"u\": \"http:\\/\\/www.spc.ncep.noaa.gov\\/\", \"t\": 1372121562, \"hc\": 1368193091, \"cy\": \"New York\", \"ll\": [ 40.862598, -73.921799 ] }");
    UpdateStream updateStream = new UpdateStream (testCaseSupplier, queue,"t");
    Thread t = new Thread(updateStream);
    t.start();
    Map<String,Object> expectedAnswer = new HashMap<String,Object>();
    expectedAnswer.put("a","Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0");
    expectedAnswer.put("c","US");
    expectedAnswer.put("nk",1);
    expectedAnswer.put ("tz", "America/New_York");
    expectedAnswer.put("gr", "NY");
    expectedAnswer.put("g","1Chgyj");
    expectedAnswer.put("h","15vMQjX");
    expectedAnswer.put("l","o_d63rn9enb");
    expectedAnswer.put("al","en-US,en;q=0.5");
    expectedAnswer.put("hh","1.usa.gov");
    expectedAnswer.put("r","http://forecast.weather.gov/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX");
    expectedAnswer.put("u","http://www.spc.ncep.noaa.gov/");
    expectedAnswer.put("t",1372121562);
    expectedAnswer.put("hc",1368193091);
    expectedAnswer.put("cy","New York");
    expectedAnswer.put("ll", Arrays.asList(40.862598, -73.921799));
    Map<String,Object> insertedRow=queue.poll(10, TimeUnit.SECONDS);
    assert expectedAnswer.equals(insertedRow);
  }





  @Test
  public void renameDimensionsCheck() throws Exception
  {
    List<String> dimensions = new ArrayList<String>();
    dimensions.add("bitly_hash");
    dimensions.add("country");
    dimensions.add("user");
    dimensions.add("city");
    dimensions.add("encoding_user_login");
    dimensions.add("short_url");
    dimensions.add("timestamp_hash");
    dimensions.add("user_bitly_hash");
    dimensions.add("url");
    dimensions.add("timezone");
    dimensions.add("time");
    dimensions.add("referring_url");
    dimensions.add("geo_region");
    dimensions.add("known_users");
    dimensions.add("accept_language");

    WebFirehoseFactory webbie = new WebFirehoseFactory("http://developer.usa.gov/1usagov",dimensions,dimensions,"time");
    Firehose webbieHose = webbie.connect();
    final int QUEUE_SIZE=2000;
    BlockingQueue<Map<String,Object>> queue = new ArrayBlockingQueue<Map<String, Object>>(QUEUE_SIZE);
    InputSupplier testCaseSupplier = new TestCaseSupplier("{ \"a\": \"Mozilla\\/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko\\/20100101 Firefox\\/21.0\", \"nk\": 1, \"tz\": \"America\\/New_York\", \"g\": \"1Chgyj\", \"h\": \"15vMQjX\", \"l\": \"o_d63rn9enb\", \"al\": \"en-US,en;q=0.5\", \"hh\": \"1.usa.gov\", \"r\": \"http:\\/\\/forecast.weather.gov\\/MapClick.php?site=okx&FcstType=text&zmx=1&zmy=1&map.x=98&map.y=200&site=OKX\", \"u\": \"http:\\/\\/www.spc.ncep.noaa.gov\\/\", \"t\": 1372121562, \"hc\": 1368193091, \"kw\": \"spcnws\", \"cy\": \"New York\", \"ll\": [ 40.862598, -73.921799 ] }");
    UpdateStream updateStream = new UpdateStream (testCaseSupplier, queue,"time");
    Thread t = new Thread(updateStream);
    t.start();
    InputRow row = webbieHose.nextRow();
    assert row.getDimensions().equals(dimensions);

  }


}
