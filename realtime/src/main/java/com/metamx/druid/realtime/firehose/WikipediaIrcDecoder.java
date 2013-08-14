package com.metamx.druid.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.druid.input.InputRow;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class WikipediaIrcDecoder implements IrcDecoder
{
  static final Pattern pattern = Pattern.compile(
      "\\x0314\\[\\[\\x0307(.+?)\\x0314\\]\\]\\x034 (.*?)\\x0310.*\\x0302(http.+?)\\x03.+\\x0303(.+?)\\x03.+\\x03 (\\(([+-]\\d+)\\).*|.+) \\x0310(.+)\\x03"
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
      "added",
      "delete",
      "delta"
  );

  final Map<String, String> namespaces;

  @JsonCreator
  public WikipediaIrcDecoder(@JsonProperty Map<String, String> namespaces)
  {
    if(namespaces == null) namespaces = Maps.newHashMap();
    this.namespaces = namespaces;
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
    boolean anonymous = ipPattern.matcher(user).matches();
    dimensions.put("user", user);

    final String flags = m.group(2);
    dimensions.put("unpatrolled", Boolean.toString(flags.contains("!")));
    dimensions.put("newPage", Boolean.toString(flags.contains("N")));
    dimensions.put("robot", Boolean.toString(flags.contains("B")));

    dimensions.put("anonymous", Boolean.toString(anonymous));

    String[] parts = page.split(":");
    if(parts.length > 1 && !parts[1].startsWith(" ")) {
      if(namespaces.containsKey(parts[0])) {
        dimensions.put("namespace", namespaces.get(parts[0]));
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
        return Lists.newArrayList(dimensions.get(dimension));
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
