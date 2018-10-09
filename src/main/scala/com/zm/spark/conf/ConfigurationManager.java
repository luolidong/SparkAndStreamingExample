package com.zm.spark.conf;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * @author luolidong
 *
 */
public class ConfigurationManager {

	private static Properties prop = new Properties();

	static {
		try {
			InputStream in = ConfigurationManager.class
					.getClassLoader().getResourceAsStream("my.properties");

//			InputStream in=new FileInputStream(new File(System.getProperty("user.dir")+File.separator+"conf"+ File.separator+"my.properties"));

			prop.load(in);  
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取指定key对应的value
	 * 
	 * @param key 
	 * @return value
	 */
	public static String getProperty(String key) {
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return value
	 */
	public static Integer getInteger(String key) {
		String value = getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取布尔类型的配置项
	 * @param key
	 * @return value
	 */
	public static Boolean getBoolean(String key) {
		String value = getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 获取Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key) {
		String value = getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0L;
	}

	public static void main(String[] args) {

		System.out.println(System.getProperty("user.dir"));

	}
}