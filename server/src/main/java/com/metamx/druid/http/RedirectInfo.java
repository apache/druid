package com.metamx.druid.http;

import java.net.URL;

/**
 */
public interface RedirectInfo
{
  public boolean doLocal();

  public URL getRedirectURL(String queryString, String requestURI);
}
