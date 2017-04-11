/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.Omni;

import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.logger.Logger;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

class WikipediaIrcDecoder implements IrcDecoder
{
  static final Logger log = new Logger(WikipediaIrcDecoder.class);

  final DatabaseReader geoLookup;

  static final Pattern pattern = Pattern.compile(
      ".*\\x0314\\[\\[\\x0307(.+?)\\x0314\\]\\]\\x034 (.*?)\\x0310.*\\x0302(http.+?)" +
      "\\x03.+\\x0303(.+?)\\x03.+\\x03 (\\(([+-]\\d+)\\).*|.+) \\x0310(.+)\\x03.*"
  );

  static final Pattern ipPattern = Pattern.compile("\\d+.\\d+.\\d+.\\d+");
  static final Pattern shortnamePattern = Pattern.compile("#(\\w\\w)\\..*");

  static final List<String> dimensionList = Lists.newArrayList(
      "page",
      "language",
      "user",
      "unpatrolled",
      "newPage",
      "robot",
      "anonymous",
      "namespace",
      "continent",
      "country",
      "region",
      "city"
  );

  final Map<String, Map<String, String>> namespaces;
  final String geoIpDatabase;

  public WikipediaIrcDecoder(Map<String, Map<String, String>> namespaces)
  {
    this(namespaces, null);
  }

  @JsonCreator
  public WikipediaIrcDecoder(
      @JsonProperty("namespaces") Map<String, Map<String, String>> namespaces,
      @JsonProperty("geoIpDatabase") String geoIpDatabase
  )
  {
    if (namespaces == null) {
      namespaces = Maps.newHashMap();
    }
    this.namespaces = namespaces;
    this.geoIpDatabase = geoIpDatabase;

    if (geoIpDatabase != null) {
      this.geoLookup = openGeoIpDb(new File(geoIpDatabase));
    } else {
      this.geoLookup = openDefaultGeoIpDb();
    }
  }

  private DatabaseReader openDefaultGeoIpDb() {
    File geoDb = new File(System.getProperty("java.io.tmpdir"),
                          this.getClass().getCanonicalName() + ".GeoLite2-City.mmdb");
    try {
      return openDefaultGeoIpDb(geoDb);
    }
    catch (RuntimeException e) {
      log.warn(e.getMessage()+" Attempting to re-download.", e);
      if (geoDb.exists() && !geoDb.delete()) {
        throw new RuntimeException("Could not delete geo db file ["+ geoDb.getAbsolutePath() +"].");
      }
      // local download may be corrupt, will retry once.
      return openDefaultGeoIpDb(geoDb);
    }
  }

  private DatabaseReader openDefaultGeoIpDb(File geoDb) {
    downloadGeoLiteDbToFile(geoDb);
    return openGeoIpDb(geoDb);
  }

  private DatabaseReader openGeoIpDb(File geoDb) {
    try {
      DatabaseReader reader = new DatabaseReader(geoDb);
      log.info("Using geo ip database at [%s].", geoDb);
      return reader;
    } catch (IOException e) {
      throw new RuntimeException("Could not open geo db at ["+ geoDb.getAbsolutePath() +"].", e);
    }
  }

  private void downloadGeoLiteDbToFile(File geoDb) {
    if (geoDb.exists()) {
      return;
    }

    try {
      log.info("Downloading geo ip database to [%s]. This may take a few minutes.", geoDb.getAbsolutePath());

      File tmpFile = File.createTempFile("druid", "geo");

      FileUtils.copyInputStreamToFile(
        new GZIPInputStream(
          new URL("http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz").openStream()
        ),
        tmpFile
      );

      if (!tmpFile.renameTo(geoDb)) {
        throw new RuntimeException("Unable to move geo file to ["+geoDb.getAbsolutePath()+"]!");
      }
    }
    catch (IOException e) {
      throw new RuntimeException("Unable to download geo ip database.", e);
    }
  }

  @JsonProperty
  public Map<String, Map<String, String>> getNamespaces()
  {
    return namespaces;
  }

  @JsonProperty
  public String getGeoIpDatabase()
  {
    return geoIpDatabase;
  }

  @Override
  public InputRow decodeMessage(final DateTime timestamp, String channel, String msg)
  {
    final Map<String, String> dimensions = Maps.newHashMap();
    final Map<String, Float> metrics = Maps.newHashMap();

    Matcher m = pattern.matcher(msg);
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid input format");
    }

    Matcher shortname = shortnamePattern.matcher(channel);
    if (shortname.matches()) {
      dimensions.put("language", shortname.group(1));
    }

    String page = m.group(1);
    String pageUrl = page.replaceAll("\\s", "_");

    dimensions.put("page", pageUrl);

    String user = m.group(4);
    Matcher ipMatch = ipPattern.matcher(user);
    boolean anonymous = ipMatch.matches();
    if (anonymous) {
      try {
        final InetAddress ip = InetAddress.getByName(ipMatch.group());
        final Omni lookup = geoLookup.omni(ip);

        dimensions.put("continent", lookup.getContinent().getName());
        dimensions.put("country", lookup.getCountry().getName());
        dimensions.put("region", lookup.getMostSpecificSubdivision().getName());
        dimensions.put("city", lookup.getCity().getName());
      }
      catch (UnknownHostException e) {
        log.error(e, "invalid ip [%s]", ipMatch.group());
      }
      catch (IOException e) {
        log.error(e, "error looking up geo ip");
      }
      catch (GeoIp2Exception e) {
        log.error(e, "error looking up geo ip");
      }
    }
    dimensions.put("user", user);

    final String flags = m.group(2);
    dimensions.put("unpatrolled", Boolean.toString(flags.contains("!")));
    dimensions.put("newPage", Boolean.toString(flags.contains("N")));
    dimensions.put("robot", Boolean.toString(flags.contains("B")));

    dimensions.put("anonymous", Boolean.toString(anonymous));

    String[] parts = page.split(":");
    if (parts.length > 1 && !parts[1].startsWith(" ")) {
      Map<String, String> channelNamespaces = namespaces.get(channel);
      if (channelNamespaces != null && channelNamespaces.containsKey(parts[0])) {
        dimensions.put("namespace", channelNamespaces.get(parts[0]));
      } else {
        dimensions.put("namespace", "wikipedia");
      }
    } else {
      dimensions.put("namespace", "article");
    }

    float delta = m.group(6) != null ? Float.parseFloat(m.group(6)) : 0;
    metrics.put("delta", delta);
    metrics.put("added", Math.max(delta, 0));
    metrics.put("deleted", Math.min(delta, 0));

    return new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return dimensionList;
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return timestamp.getMillis();
      }

      @Override
      public DateTime getTimestamp()
      {
        return timestamp;
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        final String value = dimensions.get(dimension);
        if (value != null) {
          return ImmutableList.of(value);
        } else {
          return ImmutableList.of();
        }
      }

      @Override
      public Object getRaw(String dimension)
      {
        return dimensions.get(dimension);
      }


      @Override
      public float getFloatMetric(String metric)
      {
        return metrics.get(metric);
      }

      @Override
      public long getLongMetric(String metric)
      {
        return new Float(metrics.get(metric)).longValue();
      }

      @Override
      public int compareTo(Row o)
      {
        return timestamp.compareTo(o.getTimestamp());
      }

      @Override
      public String toString()
      {
        return "WikipediaRow{" +
               "timestamp=" + timestamp +
               ", dimensions=" + dimensions +
               ", metrics=" + metrics +
               '}';
      }
    };
  }
}
