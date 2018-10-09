package com.zm.spark.test;


import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import com.zm.spark.conf.ConfigurationManager;
import com.zm.spark.constant.Constants;

import java.io.File;
import java.net.InetAddress;



public class GeoIPTest {
    public static void main(String[] args) {
// A File object pointing to your GeoIP2 or GeoLite2 database
        File database = new File("GeoLite2-City.mmdb");
        System.out.println(System.getProperty("user.dir"));

        System.out.println(ConfigurationManager.getProperty(Constants.TEST_ONLINE_HABSE_TABLE));
// This creates the DatabaseReader object. To improve performance, reuse
// the object across lookups. The object is thread-safe.
        try {
            DatabaseReader reader = new DatabaseReader.Builder(database).build();

            InetAddress ipAddress = null;
            ipAddress = InetAddress.getByName("218.9.145.159");

            CityResponse response = reader.city(ipAddress);

            Country country = response.getCountry();
            System.out.println(country.getIsoCode());            // 'US'
            System.out.println(country.getName());               // 'United States'
            System.out.println(country.getNames().get("zh-CN")); // '美国'

            Subdivision subdivision = response.getMostSpecificSubdivision();
            System.out.println(subdivision.getNames().get("zh-CN"));    // 'Minnesota'
            System.out.println(subdivision.getIsoCode()); // 'MN'
            System.out.println(subdivision.getGeoNameId());

            City city = response.getCity();
            System.out.println(city.getNames().get("zh-CN"));       // 'Minneapolis'

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
