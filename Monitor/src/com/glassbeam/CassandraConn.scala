package com.glassbeam

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.Properties
import java.io._

object CassandraConn{

	var cassandraSession:Session = null;
	val prop = new Properties();


        var Seeds=""
        var KeySpace=""

        def initialize(configPath:String):Unit={
                prop.load(new FileInputStream(configPath))
                Seeds=prop.getProperty("Seeds")
                KeySpace=prop.getProperty("KeySpace")

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
}
