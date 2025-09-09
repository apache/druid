package org.apache.druid.sql.avatica;

import org.apache.calcite.avatica.server.MetricsAwareAvaticaHandler;
import org.eclipse.jetty.server.Handler;

public abstract class DruidAvaticaHandler extends Handler.Abstract implements MetricsAwareAvaticaHandler
{
}
