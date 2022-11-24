package org.test;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期工具类
 *
 */
public class DateTimeUtils extends DateUtils {

    /**
     * Default time format :  yyyy-MM-dd HH:mm:ss
     */
    public static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * Time format :  yyyy-MM-dd HH:mm
     */
    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm";
    public static final String TIME_FORMAT = "HH:mm";

    /**
     * Default date format
     */
    public static final String DATE_FORMAT = "yyyy-MM-dd";
    /**
     * Default month format
     */
    public static final String MONTH_FORMAT = "yyyy-MM";
    /**
     * Default month format
     */
    public static final String MONTH_FORMAT_TRIM = "yyyyMM";
    /**
     * Default day format
     */
    public static final String DAY_FORMAT = "dd";


    //Date pattern,  RECOM_API_DETAIL_JSON:  2013-09-11
    public static final String DATE_PATTERN = "^[0-9]{4}\\-[0-9]{2}\\-[0-9]{2}$";


    private DateTimeUtils() {
    }

    public static boolean isDate(String dateAsText) {
        return StringUtils.isNotEmpty(dateAsText) && dateAsText.matches(DATE_PATTERN);
    }

    /**
     * 功能描述: <br>
     * 获取当前时间
     *
     * @return
     */
    public static Date now() {
        return new Date();
    }
    
    /**
     * 解析：指定日期字符串
     * 
     * @param strDate java.util.Date
     * @param strFormat      日期格式
     * @return Date
//     * @exception 发生异常时,返回0
     */
    public static Date parseStringAsDate(String strDate,String strFormat ) {
        Date date = null;
        DateFormat dateFormat = new SimpleDateFormat(strFormat);
        try {
            date = dateFormat.parse(strDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }
    
    /**
     * 功能描述: <br>
     * 指定日期字符串 yyyy-MM-dd HH:mm:ss格式
     *
     * @param strDate
     * @return
     */
    public static Date parseStringToDate(String strDate) {
        return parseStringAsDate(strDate,DEFAULT_DATE_TIME_FORMAT);
    }

    /**
     * 解析：指定Date
     * 
     * @return String
     */
    public static String parseDateAsString(Date date,String strFormat) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(strFormat);
        return  simpleDateFormat.format(date);
    }

    public static String parseDateToString(Date date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_DATE_TIME_FORMAT);
        return  simpleDateFormat.format(date);
    }
    
    public static String toDateText(Date date) {
        return toDateText(date, DATE_FORMAT);
    }

    public static String toDateText(Date date, String pattern) {
        if (date == null || pattern == null) {
            return null;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }
    


    /**
     * 日期格式转换为字符串，结果：根据格式
     *
     * @param date
     * @return
     */
    public static String dateParseString(Date date,String regix) {
        DateFormat df = new SimpleDateFormat(regix);
        return df.format(date);
    }

    /**
     * 获取给定日的最后一刻。
     * 
     * @param when
     *            给定日
     * @return 最后一刻。例如：2006-4-19 23:59:59.999
     */
    public static Date getDayEnd(Date when) {
        Date date = truncate(when, Calendar.DATE);
        date = addDays(date, 1);
        date.setTime(date.getTime() - 1);
        return date;
    }
    
    public static Date getDate(String dateText) {
        return getDate(dateText, DATE_FORMAT);
    }


    public static Date getDate(String dateText, String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        try {
            return dateFormat.parse(dateText);
        } catch (ParseException e) {
            throw new IllegalStateException("Parse date from [" + dateText + "," + pattern + "] failed", e);
        }
    }

    public static String toDateTime(Date date) {
        return toDateText(date, DATE_TIME_FORMAT);
    }

    /**
     * 日期加法
     * 
     * @param when
     *            被计算的日期
     * @param field
     *            the time field. 在Calendar中定义的常数，例如Calendar.DATE
     * @param amount
     *            加数
     * @return 计算后的日期
     */
    public static Date add(Date when, int field, int amount) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(when);
        calendar.add(field, amount);
        return calendar.getTime();
    }
    
    /**
     * 计算给定的日期加上给定的天数。
     * 
     * @param when
     *            给定的日期
     * @param amount
     *            给定的天数
     * @return 计算后的日期
     */
    public static Date addDays(Date when, int amount) {

        return add(when, Calendar.DAY_OF_YEAR, amount);
    }

    /**
     * Return current year.
     *
     * @return Current year
     */
    public static int currentYear() {
        return calendar().get(Calendar.YEAR);
    }

    public static Calendar calendar() {
        return Calendar.getInstance();
    }
    
    public static String getNowDateyyyyMMddHHmmss(){
        String strResult = "";
        String strFormat = "yyyyMMddHHmmss";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(strFormat);
        Calendar calendar = Calendar.getInstance();
        strResult = simpleDateFormat.format(calendar.getTime());
        return strResult;
    }

    public static int daysOfTwo(Date startDate, Date endDate) {
        if (null == startDate || null == endDate) {
            return -1;
        }
        return (int) ((endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24));
     }

     public static int monthOfLastDay(String yearMonth){
         SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
         try {
             Date date = format.parse(yearMonth);
             //获取当前月最后一天
             Calendar ca = Calendar.getInstance();
             ca.setTime(date);
             ca.set(Calendar.DAY_OF_MONTH, ca.getActualMaximum(Calendar.DAY_OF_MONTH));

             String last = format.format(ca.getTime());
             return Integer.valueOf(last.split("-")[2]);
         } catch (ParseException e) {
             e.printStackTrace();
         }
        return 0;
     }

        /**
         * 获取时间差（分钟,用于效率）
         * @param start 纳秒
         */
        public static Long getCostMin(Long start){
            return getCostMs(start)/(1000*60);
        }

        /**
         * 获取时间差（纳秒,用于效率）
         * @param start 纳秒
         */
        public static Long getCostMs(Long start){
            return (System.nanoTime()-start)/1000000;
        }


        /**
         * 获取时间差（纳秒,用于效率）
         * @param start 纳秒
         */
        public static Long getCostS(Long start){
            return getCostMs(start)/1000;
        }

}
