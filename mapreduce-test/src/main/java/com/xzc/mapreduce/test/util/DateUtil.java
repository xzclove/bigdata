package com.xzc.mapreduce.test.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Des 日期工具类
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年6月3日 下午3:04:23
 * @Version V1.0.0
 */
public class DateUtil {
	
	/**
	 * 返回当前年份 2015
	 * 
	 * @return
	 */
	public static String currentYear2() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
		String dateString = formatter.format(currentTime);
		return dateString;
	}

	/**
	 * 返回当前时间 HH时mm分ss秒
	 * 
	 * @return
	 */
	public static String currentDateHMS() {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
		String dateString = formatter.format(currentTime);
		return dateString;
	}
	
	public static void main(String[] args) {
		System.out.println(DateUtil.currentDateHMS());
	}

}
