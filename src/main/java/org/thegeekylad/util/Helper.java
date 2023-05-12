package org.thegeekylad.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
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

    public static boolean isDead(Thread thread) {
        return thread == null || !thread.isAlive();
    }

    public static String csvToStringBytes(File csvFile) {
        String csvString = csvToString(csvFile);
        if (csvString == null) return null;

        byte[] bytes = csvString.getBytes(StandardCharsets.UTF_8);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    public static String stringToStringBytes(String csvString) {
        if (csvString == null) return null;

        byte[] bytes = csvString.getBytes(StandardCharsets.UTF_8);
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }

    public static String stringBytesToString(String csvStringBytes) {
        if (csvStringBytes == null) return null;

        byte[] bytes = csvStringBytes.getBytes(StandardCharsets.ISO_8859_1);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static String csvToString(File csvFile) {
        try {
            StringBuilder stringBuilder = new StringBuilder();

            BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            String line;
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(System.lineSeparator());
            }

            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void stringToCsv(String csvBytesString, File outputFile) {
        byte[] bytes = csvBytesString.getBytes(StandardCharsets.ISO_8859_1);
        String csvContent = new String(bytes, StandardCharsets.UTF_8);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write(csvContent);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
