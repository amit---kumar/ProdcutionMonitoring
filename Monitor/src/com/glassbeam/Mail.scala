package com.glassbeam

import javax.mail._
import javax.mail.internet._
import java.util.Properties._
import java.util.Calendar
import java.util.Properties
import java.io._

object Mail {
	

                var ks=""
                var ec=""
	        val propb = new Properties();

        def initialize(configPath:String):Unit={
                propb.load(new FileInputStream(configPath))
                ec=propb.getProperty("ec")
                ks=propb.getProperty("KeySpace")

        }
	
	def sendMail(Textsec:String,Textevt:String,Textssn:String,Textstat:String,sst:Int,  ss:Int,  ssNF:Int,  est:Int,  es:Int,  esNF:Int,  ssns:Int,  ssnfs:Int,  ssnNF:Int,  stats:Int,  statfs:Int,  statNF:Int){
		var date=Calendar.getInstance().getTime()		
	

		// Set up the mail object
		val properties = System.getProperties
		//println(s"$properties")
		properties.put("mail.smtp.host", "localhost")
		val session = Session.getDefaultInstance(properties)
		val message = new MimeMessage(session)
		
		// Set the from, to, subject, body text
		message.setFrom(new InternetAddress("donotreply@glassbeam.com"))
		message.setRecipients(Message.RecipientType.TO, "amit.kumar@glassbeam.com,kishor.kumar@glassbeam.com,anurag.singh@glassbeam.com")
		message.setRecipients(Message.RecipientType.CC, "aklank.choudhary@glassbeam.com")
		message.setSubject("Glassbeam Daily Monitoring")
		message.setText("\n===Customer : "+ec+"\t\tKeySpace : "+ks+"\t\tDate : "+date+
		"===\n\n===Section Tables not Parsed===\n\n"+ "Total Tables : "+sst+"    Parsed Tables : "+ss +"    Not Parsed Tables : "+ssNF+"\n\n" +Textsec +
		"\n\n===Event Tables not Parsed===\n\n"+ "Total Tables : "+est+"    Parsed Tables : "+es +"    Not Parsed Tables : "+esNF+"\n\n"+ Textevt +
		"\n\n===Session Tables not Parsed===\n\n"+"Total Tables : "+ssns+"    Parsed Tables : "+ssnfs +"    Not Parsed Tables : "+ssnNF+"\n\n"+ Textssn +
		"\n\n===Stat Tables not Parsed===\n\n"+"Total Tables : "+stats+"    Parsed Tables : "+statfs +"    Not Parsed Tables : "+statNF+"\n\n"+ Textstat)
//		message.setContent("<h3><br>===Section Tables not Parsed===<br></h3>"+ Textsec +"<h3><br>===Event Tables not Parsed===<br></h3>"+ Textevt +"<h3><br>===Session Tables not Parsed===<br></h3>"+ Textssn +"<h3><br>===Stat Tables not Parsed===<br></h3>"+ Textstat,"text/html; charset=utf-8")
		
//		message.setContent(Text+"\nRegards,\n Amit", "text/html; charset=utf-8");
		// And send it
		Transport.send(message)

	}
}

