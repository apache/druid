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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.Omni;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
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

  public WikipediaIrcDecoder( Map<String, Map<String, String>> namespaces) {
    this(namespaces, null);
  }

  @JsonCreator
  public WikipediaIrcDecoder(@JsonProperty("namespaces") Map<String, Map<String, String>> namespaces,
                             @JsonProperty("geoIpDatabase") String geoIpDatabase)
  {
    if(namespaces == null) {
      namespaces = Maps.newHashMap();
    }
    this.namespaces = namespaces;


    File geoDb;
    if(geoIpDatabase != null) {
      geoDb = new File(geoIpDatabase);
    } else {
      try {
        String tmpDir = System.getProperty("java.io.tmpdir");
        geoDb = new File(tmpDir, this.getClass().getCanonicalName() + ".GeoLite2-City.mmdb");
        if(!geoDb.exists()) {
          log.info("Downloading geo ip database to [%s]", geoDb);

          FileUtils.copyInputStreamToFile(
              new GZIPInputStream(
                  new URL("http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz").openStream()
              ),
              geoDb
          );
        }
      } catch(IOException e) {
        throw new RuntimeException("Unable to download geo ip database [%s]", e);
      }
    }
    try {
      geoLookup = new DatabaseReader(geoDb);
    } catch(IOException e) {
      throw new RuntimeException("Unable to open geo ip lookup database", e);
    }
  }

  @Override
  public InputRow decodeMessage(final DateTime timestamp, String channel, String msg)
  {
    final Map<String, String> dimensions = Maps.newHashMap();
    final Map<String, Float> metrics = Maps.newHashMap();

    Matcher m = pattern.matcher(msg);
    if(!m.matches()) {
      throw new IllegalArgumentException("Invalid input format");
    }

    Matcher shortname = shortnamePattern.matcher(channel);
    if(shortname.matches()) {
      dimensions.put("language", shortname.group(1));
    }

    String page = m.group(1);
    String pageUrl = page.replaceAll("\\s", "_");

    dimensions.put("page", pageUrl);

    String user = m.group(4);
    Matcher ipMatch = ipPattern.matcher(user);
    boolean anonymous = ipMatch.matches();
    if(anonymous) {
      try {
        final InetAddress ip = InetAddress.getByName(ipMatch.group());
        final Omni lookup = geoLookup.omni(ip);

        dimensions.put("continent", lookup.getContinent().getName());
        dimensions.put("country", lookup.getCountry().getName());
        dimensions.put("region", lookup.getMostSpecificSubdivision().getName());
        dimensions.put("city", lookup.getCity().getName());
      } catch(UnknownHostException e) {
        log.error(e, "invalid ip [%s]", ipMatch.group());
      } catch(IOException e) {
        log.error(e, "error looking up geo ip");
      } catch(GeoIp2Exception e) {
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
    if(parts.length > 1 && !parts[1].startsWith(" ")) {
      Map<String, String> channelNamespaces = namespaces.get(channel);
      if(channelNamespaces != null && channelNamespaces.containsKey(parts[0])) {
        dimensions.put("namespace", channelNamespaces.get(parts[0]));
      } else {
        dimensions.put("namespace", "wikipedia");
      }
    }
    else {
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
      public List<String> getDimension(String dimension)
      {
        final String value = dimensions.get(dimension);
        if(value != null) {
          return ImmutableList.of(value);
        } else {
          return ImmutableList.of();
        }
      }

      @Override
      public float getFloatMetric(String metric)
      {
        return metrics.get(metric);
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
