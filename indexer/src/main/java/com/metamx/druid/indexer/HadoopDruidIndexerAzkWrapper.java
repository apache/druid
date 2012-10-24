package com.metamx.druid.indexer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.Granularity;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import com.metamx.druid.indexer.path.GranularityPathSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class HadoopDruidIndexerAzkWrapper
{
  private static final Logger log = new Logger(HadoopDruidIndexerAzkWrapper.class);
  private static final String PROPERTY_PREFIX = "druid.indexer.";

  private final String jobName;
  private final Properties properties;

  public HadoopDruidIndexerAzkWrapper(
      String jobName,
      Properties properties
  )
  {
    this.jobName = jobName;
    this.properties = properties;
  }

  public void run() throws Exception
  {
    final DefaultObjectMapper jsonMapper = new DefaultObjectMapper();

    final List<Interval> dataInterval;
    final Map<String, Object> theMap = Maps.newTreeMap();

    for (String propertyName : properties.stringPropertyNames()) {
      if (propertyName.startsWith(PROPERTY_PREFIX)) {
        final String propValue = properties.getProperty(propertyName);
        if (propValue.trim().startsWith("{") || propValue.trim().startsWith("[")) {
          theMap.put(propertyName.substring(PROPERTY_PREFIX.length()), jsonMapper.readValue(propValue, Object.class));
        }
        else {
          theMap.put(propertyName.substring(PROPERTY_PREFIX.length()), propValue);
        }
      }
    }

    log.info("Running with properties:");
    for (Map.Entry<String, Object> entry : theMap.entrySet()) {
      log.info("%30s => %s", entry.getKey(), entry.getValue());
    }

    dataInterval = Lists.transform(
        Lists.newArrayList(MapUtils.getString(theMap, "timeInterval").split(",")), new StringIntervalFunction()
    );

    final HadoopDruidIndexerConfig config = jsonMapper.convertValue(theMap, HadoopDruidIndexerConfig.class);
    config.setIntervals(dataInterval);
    config.setVersion(new DateTime());

    new HadoopDruidIndexerJob(config).run();
  }
}
