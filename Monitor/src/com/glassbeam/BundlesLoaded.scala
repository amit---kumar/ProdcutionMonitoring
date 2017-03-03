package com.glassbeam

import java.util.Properties
import java.io._
import java.util.Calendar
import com.datastax.driver.core.ResultSet
import grizzled.slf4j.Logging;

object BundlesLoaded extends Logging{

/*
        val propb = new Properties();
        propb.load(new FileInputStream(System.getProperty("user.dir") + "/conf/application.conf"))
        var mfr=propb.getProperty("mfr")
        var prod=propb.getProperty("prod")
        var sch=propb.getProperty("sch")*/

        val cal = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)

        val date =cal.get(Calendar.DATE)
        val Year =cal.get(Calendar.YEAR )
        val Montt =cal.get(Calendar.MONTH )
	val Month=Montt+1
	def bundles(configPath:String):List[String]={
		
        	val propb = new Properties();
	        propb.load(new FileInputStream(configPath))
	        var mfr=propb.getProperty("mfr")
        	var prod=propb.getProperty("prod")
        	var sch=propb.getProperty("sch")

	        var queryText = s"""select bundle_id from bundles_by_obs_date where  mfr='$mfr' and prod ='$prod' and sch='$sch' and obs_year=$Year and obs_month=$Month and obs_day=$date;""";
//	        var queryText = s"""select bundle_id from bundles_by_obs_date where  mfr='$mfr' and prod ='$prod' and sch='$sch' and obs_year=2016 and obs_month=8 and obs_day=15;""";
		info(s"$queryText")		
        	var Bundles = List[String]()
        	val BundleList =CassandraConn.getSession().execute(queryText);
	        while (BundleList.iterator().hasNext()) {
        	        val row = BundleList.iterator().next()
                	Bundles::= row.getString("bundle_id");
        	}

		return Bundles;
	}
}
