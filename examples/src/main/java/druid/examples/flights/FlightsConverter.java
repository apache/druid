/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package druid.examples.flights;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closeables;
import com.metamx.common.parsers.CSVParser;
import com.metamx.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 */
public class FlightsConverter
{

  private static final String[] METRIC_DIMENSIONS = new String[]{
      "Distance",
      "TaxiIn",
      "TaxiOut",
      "CarrierDelay",
      "WeatherDelay",
      "NASDelay",
      "SecurityDelay",
      "LateAircraftDelay",
      "ArrDelay",
      "DepDelay",
      "CRSElapsedTime",
      "ActualElapsedTime",
      "AirTime"
  };

  public static void main(String[] args) throws IOException
  {
    DateTimeZone.setDefault(DateTimeZone.UTC);
    ObjectMapper mapper = new DefaultObjectMapper();

    File flightsDataDirectory = new File(args[0]);
    File flightsOutputDirectory = new File(args[1]);
    flightsOutputDirectory.mkdirs();

    for (File flightsDataFile : flightsDataDirectory.listFiles()) {
      System.out.printf("Processing file[%s]%n", flightsDataFile);

      CSVParser parser = new CSVParser();
      BufferedReader in = null;
      BufferedWriter out = null;

      try {
        in = new BufferedReader(new FileReader(flightsDataFile));
        out = new BufferedWriter(
            new FileWriter(
                new File(
                    flightsOutputDirectory,
                    flightsDataFile.getName().replace("csv", "json")
                )
            )
        );

        int count = 0;
        long time = System.currentTimeMillis();
        parser.setFieldNames(in.readLine());
        String line = null;
        while ((line = in.readLine()) != null) {
          if (++count % 100000 == 0) {
            System.out.printf(
                "File[%s], processed %,d lines in %,d millis.%n",
                flightsDataFile.getName(), count, System.currentTimeMillis() - time
            );
            time = System.currentTimeMillis();
          }
          Map<String, Object> event = parser.parse(line);

          int year = Integer.parseInt(event.get("Year").toString());
          int month = Integer.parseInt(event.get("Month").toString());
          int dayOfMonth = Integer.parseInt(event.get("DayofMonth").toString());
          int departureTime = Integer.parseInt(event.get("CRSDepTime").toString());
          int hourOfDay = departureTime / 100;
          final int minuteOfHour = departureTime % 100;

          DateTime timestamp = new DateTime(String.format("%4d-%02d-%02d", year, month, dayOfMonth))
              .plus(new Period(hourOfDay, minuteOfHour, 0, 0));

          event.put("timestamp", timestamp);

          for (String metricDimension : METRIC_DIMENSIONS) {
            String value = event.get(metricDimension).toString();

            if (value.equals("NA")) {
              event.put(metricDimension, 0);
            }
            else {
              event.put(metricDimension, Integer.parseInt(value));
            }
          }

          out.write(mapper.writeValueAsString(event));
          out.write("\n");
        }
      }
      finally {
        Closeables.closeQuietly(in);
        Closeables.closeQuietly(out);
      }
    }
  }
}
