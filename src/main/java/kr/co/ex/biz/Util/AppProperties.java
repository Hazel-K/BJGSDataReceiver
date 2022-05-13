package kr.co.ex.biz.Util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppProperties {
	private static final Logger log = LoggerFactory.getLogger(AppProperties.class);
	private Properties prop;	
	
	private AppProperties() {
		String resource = "/application.properties"; // Path for local Project
//		String resource = ""; // Path for runnable Jar File
		prop = new Properties();
		
		try {
			InputStream reader = getClass().getResourceAsStream(resource); // Path for local Project
//			FileReader reader = new FileReader(new File(resource)); // Path for runnable Jar File
			prop.load(reader);
		} catch(IOException e) {
			log.error(e.getMessage());
		}
	}
	
	private static class Initial {
		private static final AppProperties instance = new AppProperties();
	}
	
	public static AppProperties getInstance() {
		return Initial.instance;
	}
	
	public String getProp(String key) {
		return prop.getProperty(key);
	}
	
	public void setProp(String key, String value) {
		prop.setProperty(key, value);
	}
}
