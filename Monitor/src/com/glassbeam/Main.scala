package com.glassbeam

import grizzled.slf4j.Logging

object Main extends App with Logging {
	
	info("Welcome To Monitoring");
	println(args.isEmpty)
	println(s"$args")
	var configPath:String=args(0)
	println(configPath)
	CassandraConn.initialize(configPath)
    	val Bundles =BundlesLoaded.bundles(configPath);
	//Bundles.foreach(e=>info(e))
	Metadata.initialize(configPath)
        val SectionTables=Metadata.sectionTbl();
	//SectionTables.foreach(e=>info(e))
        val EventTables=Metadata.eventTbl();
        EventTables.foreach(e=>info(e))
        val SessionTables=Metadata.sessionTbl();
        //SessionTables.foreach(e=>info(e))
        val StatTables=Metadata.statTbl();
        //StatTables.foreach(e=>info(e))
	info("===========Querying Section Tables========== ")
	SearchCass.initialize(configPath)
	val sectblF=SearchCass.sectionTbl(Bundles,SectionTables);

	info("Checking Completed")
	var sst=SectionTables.size
        info()
	info(s"Total Section Table size=$sst")
        info()
        info("===========Below Tables are Parsed========== ")
	var ss=sectblF.size
	info(s"size=$ss")
	sectblF.foreach(e=>info(e))

	var SectionTablesSet=SectionTables.toSet
	var sectblNF=SectionTablesSet.diff(sectblF)
	var ssNF=sectblNF.size
	info()
	info("===========Below Tables are not Parsed========== ")
        info(s"size=$ssNF")
        sectblNF.foreach(e=>info(e))
	var Textsec=sectblNF.mkString(",    ")
//	Mail.sendMail(Textsec);

	info("===========Querying Event Tables========== ")
       	val evttblF=SearchCass.eventTbl(Bundles,EventTables);

        info("Checking Completed")
        var est=EventTables.size
        info()
        info(s"Total Event Table size=$est")
        info()
        info("===========Below Tables are Parsed========== ")
        var es=evttblF.size
        info(s"size=$es")
        evttblF.foreach(e=>info(e))

        var EventTablesSet=EventTables.toSet
        var evttblNF=EventTablesSet.diff(evttblF)
        var esNF=evttblNF.size
        info()
        info("===========Below Tables are not Parsed========== ")
        info(s"size=$esNF")
        evttblNF.foreach(e=>info(e))
	var Textevt=evttblNF.mkString(",    ")	
//	Mail.sendMail(Textevt);


	info("===========Querying Session Tables are not Parsed========== ")
        val ssntblF=SearchCass.sessionTbl(Bundles,SessionTables);

        info("Checking Completed")
        var ssns=SessionTables.size
        info()
        info(s"Total Session Table size=$ssns")
        info()
        info("===========Below Tables are Parsed========== ")
        var ssnfs=ssntblF.size
        info(s"size=$ssnfs")
        ssntblF.foreach(e=>info(e))

        var SessionTablesSet=SessionTables.toSet
        var ssntblNF=SessionTablesSet.diff(ssntblF)
        var ssnNF=ssntblNF.size
        info()
        info("===========Below Tables are not Parsed========== ")
        info(s"size=$ssnNF")
        ssntblNF.foreach(e=>info(e))
	var Textssn=ssntblNF.mkString(",    ")	

	info("===========Querying Stat Tables are not Parsed========== ")
        val stattblF=SearchCass.statTbl(Bundles,StatTables);

        info("Checking Completed")
        var stats=StatTables.size
        info()
        info(s"Total Stat Table size=$stats")
        info()
        info("===========Below Tables are Parsed========== ")
        var statfs=stattblF.size
        info(s"size=$statfs")
        stattblF.foreach(e=>info(e))

        var StatTablesSet=StatTables.toSet
        var stattblNF=StatTablesSet.diff(stattblF)
        var statNF=stattblNF.size
        info()
        info("===========Below Tables are not Parsed========== ")
        info(s"size=$statNF")
        stattblNF.foreach(e=>info(e))
	var Textstat=stattblNF.mkString(",    ")


	Mail.initialize(configPath)
	Mail.sendMail(Textsec,Textevt,Textssn,Textstat,sst,ss,ssNF,est,es,esNF,ssns,ssnfs,ssnNF,stats,statfs,statNF);
	
	sys.exit(0)

}
