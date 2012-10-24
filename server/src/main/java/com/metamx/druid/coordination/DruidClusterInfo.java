package com.metamx.druid.coordination;

import com.metamx.druid.master.DruidMaster;
import com.metamx.phonebook.PhoneBook;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class DruidClusterInfo
{
  private final DruidClusterInfoConfig config;
  private final PhoneBook yp;

  public DruidClusterInfo(
      DruidClusterInfoConfig config,
      PhoneBook zkPhoneBook
  )
  {
    this.config = config;
    this.yp = zkPhoneBook;
  }

  public Map<String, String> lookupCurrentLeader()
  {
    return (Map<String, String>) yp.lookup(
        yp.combineParts(Arrays.asList(config.getMasterPath(), DruidMaster.MASTER_OWNER_NODE)), Map.class
    );
  }

  public String getMasterHost()
  {
    return lookupCurrentLeader().get("host");
  }
}
