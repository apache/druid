package com.metamx.druid;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;

/**
 */
public abstract class DruidNode
{
  private static final Logger log = new Logger(DruidNode.class);

  private final ObjectMapper jsonMapper;
  private final Lifecycle lifecycle;
  private final Properties props;
  private final ConfigurationObjectFactory configFactory;

  private boolean initialized = false;

  public DruidNode(
      ObjectMapper jsonMapper,
      Lifecycle lifecycle,
      Properties props,
      ConfigurationObjectFactory configFactory
  )
  {
    this.jsonMapper = jsonMapper;
    this.lifecycle = lifecycle;
    this.props = props;
    this.configFactory = configFactory;
  }

  public abstract void init();

  @LifecycleStart
  public synchronized void start() throws Exception
  {
    if (!initialized) {
      init();
    }

    lifecycle.start();
  }

  @LifecycleStop
  public synchronized void stop()
  {
    lifecycle.stop();
  }
}
