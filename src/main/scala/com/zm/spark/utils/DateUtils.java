package com.zm.spark.utils;

import org.apache.commons.lang.time.FastDateFormat;

import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 * @author Luolidong
 *
 */
public class DateUtils {

//    public static final SimpleDateFormat TIME_FORMAT =
//            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    public static final SimpleDateFormat DATE_FORMAT =
//            new SimpleDateFormat("yyyy-MM-dd");
//    public static final SimpleDateFormat DATEKEY_FORMAT =
//            new SimpleDateFormat("yyyyMMdd");
//    public static final SimpleDateFormat MIN_FORMAT =
//            new SimpleDateFormat("yyyy-MM-dd HH:mm");

    public static final FastDateFormat FAST_TIME_FORMAT  = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    public static final FastDateFormat FAST_DATE_FORMAT  = FastDateFormat.getInstance("yyyy-MM-dd");
    public static final FastDateFormat FAST_DATEKEY_FORMAT  = FastDateFormat.getInstance("yyyyMMdd");
    public static final FastDateFormat FAST_MIN_FORMAT  = FastDateFormat.getInstance("yyyy-MM-dd HH:mm");
    public static final FastDateFormat FAST_HOUR_FORMAT  = FastDateFormat.getInstance("yyyy-MM-dd HH");

//    /**
//     * 判断一个时间是否在另一个时间之前
//     * @param time1 第一个时间
//     * @param time2 第二个时间
//     * @return 判断结果
//     */
//    public static boolean before(String time1, String time2) {
//        try {
//            Date dateTime1 = TIME_FORMAT.parse(time1);
//            Date dateTime2 = TIME_FORMAT.parse(time2);
//
//            if(dateTime1.before(dateTime2)) {
//                return true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return false;
//    }

//    /**
//     * 判断一个时间是否在另一个时间之后
//     * @param time1 第一个时间
//     * @param time2 第二个时间
//     * @return 判断结果
//     */
//    public static boolean after(String time1, String time2) {
//        try {
//            Date dateTime1 = TIME_FORMAT.parse(time1);
//            Date dateTime2 = TIME_FORMAT.parse(time2);
//
//            if(dateTime1.after(dateTime2)) {
//                return true;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return false;
//    }

//    /**
//     * 计算时间差值（单位为秒）
//     * @param time1 时间1
//     * @param time2 时间2
//     * @return 差值
//     */
//    public static int minus(String time1, String time2) {
//        try {
//            Date datetime1 = TIME_FORMAT.parse(time1);
//            Date datetime2 = TIME_FORMAT.parse(time2);
//
//            long millisecond = datetime1.getTime() - datetime2.getTime();
//
//            return Integer.valueOf(String.valueOf(millisecond / 1000));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return 0;
//    }

    /**
     * 获取年月日和小时
     * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String datetime) {
        String date = datetime.split(" ")[0];
        String hourMinuteSecond = datetime.split(" ")[1];
        String hour = hourMinuteSecond.split(":")[0];
        return date + "_" + hour;
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     * @return 当天日期
     */
    public static String getTodayDate() {
        return FAST_DATE_FORMAT.format(new Date());
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.add(Calendar.DAY_OF_YEAR, -1);

        Date date = cal.getTime();

        return FAST_DATE_FORMAT.format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatTime(Date date) {
        return FAST_TIME_FORMAT.format(date);
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {return FAST_DATE_FORMAT.format(date);}

    /**
     * 格式化日期 (yyyyMMdd)
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDateKey(Date date) {return FAST_DATEKEY_FORMAT.format(date);}

    /**
     * 格式化日期 (yyyy-MM-dd HH:mm)
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatMin(Date date) {return FAST_MIN_FORMAT.format(date);}

    /**
     * 解析时间字符串
     * @param time 时间字符串
     * @return 格式化后的日期（yyyy-MM-dd HH:mm:ss）
     */
    public static String parseTime(String time) {return FAST_TIME_FORMAT.format(new Long(time) * 1000L);}

    /**
     * 解析时间字符串
     * @param time 时间字符串
     * @return 格式化后的日期（yyyy-MM-dd）
     */
    public static String parseDate(String time) {return FAST_DATE_FORMAT.format(new Long(time) * 1000L);}

    /**
     * 解析时间字符串
     * @param time 时间字符串
     * @return 格式化后的日期（yyyyMMdd）
     */
    public static String parseDateKey(String time) {return FAST_DATEKEY_FORMAT.format(new Long(time) * 1000L);}

    /**
     * 解析时间字符串
     * @param time 时间字符串
     * @return 格式化后的日期（yyyy-MM-dd HH:mm）
     */
    public static String parseMin(String time) {return FAST_MIN_FORMAT.format(new Long(time) * 1000L);}

    /**
     * 解析时间字符串
     * @param time 时间字符串
     * @return 格式化后的日期（yyyy-MM-dd HH）
     */
    public static String parseHour(String time) {return FAST_HOUR_FORMAT.format(new Long(time) * 1000L);}
}
