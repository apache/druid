package com.metamx.druid;

import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.skife.config.ConfigurationObjectFactory;

import java.util.Properties;

/**
 */
@Deprecated
public abstract class BaseNode<T extends BaseNode> extends QueryableNode
{
  protected BaseNode(
      Logger log,
      Properties props,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      ConfigurationObjectFactory configFactory
  )
  {
    super(log, props, lifecycle, jsonMapper, smileMapper, configFactory);
  }
}
