package com.glassbeam

import java.util.Properties
import java.io._
import java.util.Calendar
import com.datastax.driver.core.ResultSet
import java.text.SimpleDateFormat
import java.util.Date
import com.datastax.driver.core.Row
import grizzled.slf4j.Logging;

object SearchCass extends Logging {

        val propb = new Properties();
        var mfr=""
        var prod=""
        var sch=""
	var ec=""
	
	var sectblNF=Set[String]()
	var evttblNF=Set[String]()
	var ssntblNF=Set[String]()
	var stattblNF=Set[String]()

	val cale = Calendar.getInstance()
	cale.add(Calendar.DATE, -1)
        var sdfe = new SimpleDateFormat("yyyy-MM-dd");
	var sde=sdfe.format(cale.getTime())
        var obs_dt=sde+" 00:00:00+0000"
	var evt_dt=obs_dt
	var sess_dt=evt_dt
	var stat_dt=evt_dt

        def initialize(configPath:String):Unit={
                propb.load(new FileInputStream(configPath))
                mfr=propb.getProperty("mfr")
                prod=propb.getProperty("prod")
                sch=propb.getProperty("sch")
		ec=propb.getProperty("ec")
        }

		
	def sectionTbl( Bundles: List[String],SectionTables: List[String]):Set[String]={
		var l=Bundles.length
		var i=0;
		for( b<-Bundles ){
			info(s"$b")
			i=i+1
			var reg="^\\S+___\\d+".r
			if(b.matches(reg.toString)){			
			info(s"length=$l and remaining=$l-$i")	
			var parts = b.split("___");
			var part1 = parts(0);
			var part2 = parts(1);
			info(s"$part1 $part2")
/*			var e=part2.toLong
			var sdf = new SimpleDateFormat("yyyy-MM-dd");
			var sd=sdf.format(new Date(e));
			var obs_dt=sd+" 00:00:00+0000"*/
			for( t<-SectionTables ){
 				var queryText = s"""select count(*) from section_tbl where mfr='$mfr' and prod='$prod' and sch='$sch' and ec='$ec' and sysid='$part1' and tbl='$t' and obs_dt='$obs_dt' limit 2;""";		
				info(s"$queryText");
			        var countr:Row=CassandraConn.getSession().execute(queryText).one();
				var count = countr.getLong("count");
				info(s"count=$count")
				if(count!=0)
					sectblNF+=t
				var ss=sectblNF.size
				info(s"size in search=$ss") 
			}
		        }
		}
		return sectblNF
	}
	
        def eventTbl( Bundles: List[String],EventTables: List[String]):Set[String]={
                var l=Bundles.length
                var i=0;
                for( b<-Bundles ){
                        info(s"$b")
                        i=i+1
                        var reg="^\\S+___\\d+".r
                        if(b.matches(reg.toString)){
			info(s"length=$l and remaining=$l-$i")
                        var parts = b.split("___");
                        var part1 = parts(0);
                        var part2 = parts(1);
                        info(s"$part1 $part2")
        /*                var e=part2.toLong
                        var sdf = new SimpleDateFormat("yyyy-MM-dd");
                        var sd=sdf.format(new Date(e));
                        var evt_dt=sd+" 00:00:00+0000"*/
                        for( t<-EventTables ){
                                var queryText = s"""select count(*) from event_tbl  WHERE TOKEN(mfr , prod , sch , ec , sysid , tbl ,evt_dt ) = token('$mfr','$prod','$sch', '$ec' , '$part1', '$t', '$evt_dt') limit 2;""";
//                                var queryText = s"""select count(*) from event_tbl  WHERE TOKEN(mfr , prod , sch , ec , sysid , tbl ,evt_dt ) = token('$mfr','$prod','$sch', '$ec' , '$part1', '$t', '2015-02-12 00:00:00+0000');""";
                                info(s"$queryText");
                                var countr:Row=CassandraConn.getSession().execute(queryText).one();
                                var count = countr.getLong("count");
                                info(s"count=$count")
                                if(count!=0)
                                        evttblNF+=t
                                var ss=evttblNF.size
                                info(s"size in search=$ss")
                        }
			}
                }
                return evttblNF
        }

        def sessionTbl( Bundles: List[String],SessionTables: List[String]):Set[String]={
                var l=Bundles.length
                var i=0;
                for( b<-Bundles ){
                        info(s"$b")
                        i=i+1
                        var reg="^\\S+___\\d+".r
                        if(b.matches(reg.toString)){
                        info(s"length=$l and remaining=$l-$i")
                        var parts = b.split("___");
                        var part1 = parts(0);
                        var part2 = parts(1);
                        info(s"$part1 $part2")
                       /* var e=part2.toLong
                        var sdf = new SimpleDateFormat("yyyy-MM-dd");
                        var sd=sdf.format(new Date(e));
                        var sess_dt=sd+" 00:00:00+0000"*/
                        for( t<-SessionTables ){
                                var queryText = s"""select count(*) from sess_attr_tbl where mfr='$mfr' and prod='$prod' and sch='$sch' and ec='$ec' and sysid='$part1' and tbl='$t' and sess_dt='$sess_dt' limit 2;""";
                                info(s"$queryText");
                                var countr:Row=CassandraConn.getSession().execute(queryText).one();
                                var count = countr.getLong("count");
                                info(s"count=$count")
                                if(count!=0)
                                        ssntblNF+=t
                                var ss=ssntblNF.size
                                info(s"size in search=$ss")
                        }
			}
                }
                return ssntblNF
        }

        def statTbl( Bundles: List[String],StatTables: List[String]):Set[String]={
                var l=Bundles.length
                var i=0;
                for( b<-Bundles ){
                        info(s"$b")
                        i=i+1
                        var reg="^\\S+___\\d+".r
                        if(b.matches(reg.toString)){
                        info(s"length=$l and remaining=$l-$i")
                        var parts = b.split("___");
                        var part1 = parts(0);
                        var part2 = parts(1);
                        info(s"$part1 $part2")
                       /* var e=part2.toLong
                        var sdf = new SimpleDateFormat("yyyy-MM-dd");
                        var sd=sdf.format(new Date(e));
                        var stat_dt=sd+" 00:00:00+0000"*/
                        for( t<-StatTables ){
                                var queryText = s"""select count(*) from  numeric_cols_by_time where mfr='$mfr' and prod='$prod' and sch='$sch' and ec='$ec' and sysid='$part1' and tbl='$t' and stat_dt='$stat_dt' limit 2;""";
                                info(s"$queryText");
                                var countr:Row=CassandraConn.getSession().execute(queryText).one();
                                var count = countr.getLong("count");
                                info(s"count=$count")
                                if(count!=0)
                                        stattblNF+=t
                                var ss=stattblNF.size
                                info(s"size in search=$ss")
                        }
			}
                }
                return stattblNF
        }

}
