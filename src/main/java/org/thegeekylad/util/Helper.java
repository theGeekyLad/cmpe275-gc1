package org.thegeekylad.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class Helper {
    public static String getTimestampString(Date date) {
        String timestamp;
        if (date != null)
            timestamp = ZonedDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()).format(DateTimeFormatter.ISO_INSTANT);
        else
            timestamp = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);
        return timestamp;
    }

    public static Date getDate(String timestamp) {
        return Date.from(Instant.from(DateTimeFormatter.ISO_INSTANT.parse(timestamp)));
    }
}
