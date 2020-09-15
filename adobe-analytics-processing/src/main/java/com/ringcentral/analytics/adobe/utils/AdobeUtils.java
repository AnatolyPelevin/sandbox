package com.ringcentral.analytics.adobe.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class AdobeUtils {
    public static <T extends Enum<T>> T getEnumFromString(Class<T> c, String string) {
        if (c != null && string != null) {
            try {
                return Enum.valueOf(c, string.trim().toUpperCase());
            } catch (IllegalArgumentException ex) {
            }
        }
        return null;
    }

    public static List<?> convertObjectToList(Object obj) {
        List<?> list = new ArrayList<>();
        if (obj.getClass().isArray()) {
            list = Arrays.asList((Object[]) obj);
        } else if (obj instanceof Collection) {
            list = new ArrayList<>((Collection<?>) obj);
        }
        return list;
    }

    public static boolean isCollection(Object obj) {
        return obj.getClass().isArray() || obj instanceof Collection;
    }


    public static <T> List<T> concatenate(List<T>... lists) {
        List<T> result = new ArrayList<>();
        Stream.of(lists).forEach(result::addAll);

        return result;
    }

    public static <T> List<T> concatenate(List<List<T>> lists) {
        List<T> result = new ArrayList<>();
        lists.forEach(result::addAll);

        return result;
    }

    /**
     * convert  date string in format = "May 12, 2019" into ISO 8601 format 2019-05-12
     *
     * @param dateString - date string in format = "May 12, 2019"
     * @return date in ISO8601 format
     */
    public static LocalDate dateConverter(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("LLL d, yyyy", Locale.ENGLISH);
        return LocalDate.parse(dateString, formatter);
    }

    /**
     * replace date filter in a report's body
     */
    public static String getDateRange(String dateRange) {
        final String timeStamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.mmm").format(Calendar.getInstance().getTime());
        return dateRange.replaceFirst("/.*", "/" + timeStamp);
    }

}
