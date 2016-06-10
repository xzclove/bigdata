package com.xzc.flume.test.util;

import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Des 读取配置文件工具类
 * @Author feelingxu@tcl.com:
 * @Date 创建时间：2016年5月4日 下午1:27:16
 * @Version V1.0.0
 */
public class ConfigHolder {

	private static final Logger logger = LoggerFactory.getLogger(ConfigHolder.class);
	private static volatile ResourceBundle property;

	private static void inputConfig() {
		try {
			property = ResourceBundle.getBundle("config", Locale.CHINESE);
		} catch (Exception e) {
			logger.error(String.format("Fail load config %s, cause: %s", new Object[] { "config", e.getMessage() }));
		}

	}

	public static String getConfig(String key, String defaultValue) {
		if (property == null) {
			return defaultValue;
		}
		String value = property.getString(key);
		return StringUtils.isEmpty(value) ? defaultValue : value;
	}

	public static String getConfig(String key) {
		return getConfig(key, null);
	}

	public static int getConfig(String key, int defaultValue) {
		String value = getConfig(key);
		return StringUtils.isEmpty(value) ? defaultValue : Integer.valueOf(value).intValue();
	}

	static {
		inputConfig();
	}
}