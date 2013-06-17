package com.metamx.druid.http;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.druid.master.DruidMaster;

import java.net.URL;

/**
*/
public class MasterRedirectInfo implements RedirectInfo
{
  private final DruidMaster master;

  @Inject
  public MasterRedirectInfo(DruidMaster master) {
    this.master = master;
  }

  @Override
  public boolean doLocal()
  {
    return master.isClusterMaster();
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    try {
      final String currentMaster = master.getCurrentMaster();
      if (currentMaster == null) {
        return null;
      }

      String location = String.format("http://%s%s", currentMaster, requestURI);

      if (queryString != null) {
        location = String.format("%s?%s", location, queryString);
      }

      return new URL(location);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
