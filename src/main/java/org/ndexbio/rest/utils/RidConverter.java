package org.ndexbio.rest.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 9/26/13
 */
public class RidConverter {
  public static ORID convertToRID(String id) {
    final Matcher m = Pattern.compile("^C(\\d*)R(\\d*)$").matcher(id.trim());

    if (m.matches())
      return new ORecordId(Integer.valueOf(m.group(1)), OClusterPositionFactory.INSTANCE.valueOf(m.group(2)));
    else
      throw new RuntimeException(id + " is not valid jid");
  }

  public static String convertFromRID(ORID rid) {
    return rid.toString().replace("#", "C").replace(":", "R");
  }

}
