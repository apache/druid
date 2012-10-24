package com.metamx.druid.utils;

import com.metamx.common.ISE;

import java.util.Properties;

/**
 */
public class PropUtils
{
  public static String getProperty(Properties props, String property)
  {
    String retVal = props.getProperty(property);

    if (retVal == null) {
      throw new ISE("Property[%s] not specified.", property);
    }

    return retVal;
  }

  public static int getPropertyAsInt(Properties props, String property)
  {
    return getPropertyAsInt(props, property, null);
  }

  public static int getPropertyAsInt(Properties props, String property, Integer defaultValue)
  {
    String retVal = props.getProperty(property);

    if (retVal == null) {
      if (defaultValue == null) {
        throw new ISE("Property[%s] not specified.", property);
      }
      else {
        return defaultValue;
      }
    }

    try {
      return Integer.parseInt(retVal);
    }
    catch (NumberFormatException e) {
      throw new ISE(e, "Property[%s] is expected to be an int, it is not[%s].", retVal);
    }
  }
}
