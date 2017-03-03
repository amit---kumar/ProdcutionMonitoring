package com.glassbeam

import java.util.Properties
import java.io._
import java.util.Calendar
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


object Metadata{

        val propm = new Properties();
        var mfr=""
        var prod=""
        var sch=""

        var cassandraSession:Session = null;
        var Seeds=""
        var KeySpace=""

	def initialize(configPath:String):Unit={
                propm.load(new FileInputStream(configPath))
                mfr=propm.getProperty("mfr")
                prod=propm.getProperty("prod")
                sch=propm.getProperty("sch")
		Seeds=propm.getProperty("Seeds")
	        KeySpace=propm.getProperty("MetaKeySpace")

	}

        def getSession():Session= {
                if (cassandraSession == null) {
                        cassandraSession = createSession();
                }
                return cassandraSession;
        }

        def createSession():Session=  {
		
                var cluster:Cluster = Cluster.builder().addContactPoint(Seeds).build();
                return cluster.connect(KeySpace);
        }

	def sectionTbl():List[String]={
                var queryText = s"""select tbl from metadata_table where  mfr='$mfr' and prod ='$prod' and sch='$sch' and ns_type='SECTION';""";
		var Tables = List[String]()
                val TableList =Metadata.getSession().execute(queryText);
                while (TableList.iterator().hasNext()) {
                        val row = TableList.iterator().next()
                        Tables::= row.getString("tbl");
                }
                return Tables;
	}
        def eventTbl():List[String]={
                var queryText = s"""select tbl from metadata_table where  mfr='$mfr' and prod ='$prod' and sch='$sch' and ns_type='EVENT';""";
                var Tables = List[String]()
                val TableList =Metadata.getSession().execute(queryText);
                while (TableList.iterator().hasNext()) {
                        val row = TableList.iterator().next()
                        Tables::= row.getString("tbl");
                }
                return Tables;
        }
        def sessionTbl():List[String]={
                var queryText = s"""select tbl from metadata_table where  mfr='$mfr' and prod ='$prod' and sch='$sch' and ns_type='SESSION';""";
                var Tables = List[String]()
                val TableList =Metadata.getSession().execute(queryText);
                while (TableList.iterator().hasNext()) {
                        val row = TableList.iterator().next()
                        Tables::= row.getString("tbl");
                }
                return Tables;
        }
        def statTbl():List[String]={
                var queryText = s"""select tbl from metadata_table where  mfr='$mfr' and prod ='$prod' and sch='$sch' and ns_type='STAT';""";
                var Tables = List[String]()
                val TableList =Metadata.getSession().execute(queryText);
                while (TableList.iterator().hasNext()) {
                        val row = TableList.iterator().next()
                        Tables::= row.getString("tbl");
                }
                return Tables;
        }

}
