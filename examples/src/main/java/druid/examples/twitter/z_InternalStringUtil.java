package druid.examples.twitter;

/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.ArrayList;
import java.util.List;


import java.util.ArrayList;
import java.util.List;

/**
 * @author Yusuke Yamamoto - yusuke at mac.com
 * @since Twitter4J 2.1.4
 */
public class z_InternalStringUtil {
  private z_InternalStringUtil() {
    throw new AssertionError();
  }

  public static String maskString(String str) {
    StringBuilder buf = new StringBuilder(str.length());
    for (int i = 0; i < str.length(); i++) {
      buf.append("*");
    }
    return buf.toString();
  }

  // for JDK1.4 compatibility

  public static String[] split(String str, String separator) {
    String[] returnValue;
    int index = str.indexOf(separator);
    if (index == -1) {
      returnValue = new String[]{str};
    } else {
      List<String> strList = new ArrayList<String>();
      int oldIndex = 0;
      while (index != -1) {
        String subStr = str.substring(oldIndex, index);
        strList.add(subStr);
        oldIndex = index + separator.length();
        index = str.indexOf(separator, oldIndex);
      }
      if (oldIndex != str.length()) {
        strList.add(str.substring(oldIndex));
      }
      returnValue = strList.toArray(new String[strList.size()]);
    }

    return returnValue;
  }

  public static String join(int[] follows) {
    StringBuilder buf = new StringBuilder(11 * follows.length);
    for (int follow : follows) {
      if (0 != buf.length()) {
        buf.append(",");
      }
      buf.append(follow);
    }
    return buf.toString();
  }

  public static String join(long[] follows) {
    StringBuilder buf = new StringBuilder(11 * follows.length);
    for (long follow : follows) {
      if (0 != buf.length()) {
        buf.append(",");
      }
      buf.append(follow);
    }
    return buf.toString();
  }

  public static String join(String[] track) {
    StringBuilder buf = new StringBuilder(11 * track.length);
    for (String str : track) {
      if (0 != buf.length()) {
        buf.append(",");
      }
      buf.append(str);
    }
    return buf.toString();
  }

  public static String join(List<String> strs) {
    StringBuilder buf = new StringBuilder(11 * strs.size());
    for (String str : strs) {
      if (0 != buf.length()) {
        buf.append(",");
      }
      buf.append(str);
    }
    return buf.toString();
  }
}
