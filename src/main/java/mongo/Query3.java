package mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.bson.Document;


public class Query3 {
	public static void main(String[] args) {
		
		String uriString = "mongodb://35.192.174.210:"+27017;

		// Connecting to mongo

		MongoClientURI uri = new MongoClientURI(uriString);
		MongoClient mongo = new MongoClient(uri);
		MongoDatabase database = mongo.getDatabase("config");
		Instant startinstance = Instant.now();
		MongoCollection<Document> collection = database.getCollection("stationcollection2");
		FindIterable<Document> stations = collection.find();
		ArrayList<String> detectorids = new ArrayList<String>();
		double stationLength = 0;
		for(Document station : stations) {
			//ArrayList<Document> arr = (ArrayList<Document>) station.get("stations");
			//for(Document station : arr) {
				if(String.valueOf(station.get("locationtext")).equalsIgnoreCase("Foster NB")) {
					stationLength = Double.parseDouble((String) station.get("length"));
					ArrayList<Document> arr1 = (ArrayList<Document>) station.get("detectors");
					for(Document detector : arr1) {
						detectorids.add(String.valueOf(detector.get("detectorid")));
					}
				}
			//}
		}
		Set<String> tokens = new HashSet<String>();
		for(String s : detectorids) {
			tokens.add(s);
		}
		System.out.println("Detector id's: "+Arrays.toString(detectorids.toArray()));
		
		
		 // PART 2 : Now query the loopdata to find the detectors
		
		
		MongoCollection<Document> loopcollection = database.getCollection("loopdata2");
		int sum = 0;
		Date startDate1 = null;
		Date endDate1 = null;
		Date startDate2 = null;
		Date endDate2 = null;
		try {
			startDate1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 07:00:00");
			endDate1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 09:00:00");
			startDate2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 16:00:00");
			endDate2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 18:00:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FindIterable<Document> loopdata1 = loopcollection.find(Filters
				.and(Filters.in("detectorid", tokens), Filters.gte("starttime", startDate1), Filters.lte("starttime", endDate1)));
		FindIterable<Document> loopdata2 = loopcollection.find(Filters
				.and(Filters.in("detectorid", tokens), Filters.gte("starttime", startDate2), Filters.lte("starttime", endDate2)));
		long count = 0;
		for(Document loop : loopdata1) {
			if(loop.get("speed") != null) {
				String string= loop.get("volume").toString();
				 if(string.equals("") || string.equals(null) || string.equals(" ")) continue;
				 int volume = Integer.parseInt(string);
				if(volume > 0) {
					String string2= loop.get("speed").toString();
					if(string2.equals("") || string2.equals(null) || string2.equals(" ")) continue;
					sum += Integer.parseInt(string2);
					count++;
				}
			}
		}
		for(Document loop : loopdata2) {
			if(loop.get("speed") != null) {
				String string= loop.get("volume").toString();
				 if(string.equals("") || string.equals(null) || string.equals(" ")) continue;
				 int volume = Integer.parseInt(string);
				if(volume > 0) {
					String string2= loop.get("speed").toString();
					if(string2.equals("") || string2.equals(null) || string2.equals(" ")) continue;
					sum += Integer.parseInt(string2);
					count++;
				}
			}
		}
		System.out.println("Sum of speeds: "+sum);
		System.out.println("Count: "+count);
		double avg = (double)sum/count;
		System.out.println("Average speed: "+ avg);
		System.out.println("Length of the staion: "+stationLength);
		System.out.println("Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for station Foster NB in minutes: "+ (stationLength/avg) * 60);
		Instant finishinstance = Instant.now();
		long timeElapsed = Duration.between(startinstance, finishinstance).toMillis();  //in millis
		System.out.println("time taken:"+timeElapsed);
		// close db connection
		mongo.close();
	}
	
}
