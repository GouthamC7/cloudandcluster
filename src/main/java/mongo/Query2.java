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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.bson.Document;


// Volume: Find the total volume for the station Foster NB for Sept 15, 2011.


public class Query2 {
	public static void main(String[] args) {
		String uriString = "mongodb://35.239.16.148:"+27017;

		// Connecting to mongo

		MongoClientURI uri = new MongoClientURI(uriString);
		MongoClient mongo = new MongoClient(uri);
		MongoDatabase database = mongo.getDatabase("config");
		Instant startinstance = Instant.now();
		MongoCollection<Document> collection = database.getCollection("stationcollection");
		FindIterable<Document> stations = collection.find();
		ArrayList<String> detectorids = new ArrayList<String>();
		
		// Step1: Getting detectors for station 'Foster NB' 
		
		for(Document station : stations) {
				if(String.valueOf(station.get("locationtext")).equalsIgnoreCase("Foster NB")) {
					ArrayList<Document> arr1 = (ArrayList<Document>) station.get("detectors");
					for(Document detector : arr1) {
						detectorids.add(String.valueOf(detector.get("detectorid")));
					}
				}
		}
		Set<String> tokens = new HashSet<String>();
		for(String s : detectorids) {
			tokens.add(s);
		}
		System.out.println("Detector id's: "+Arrays.toString(detectorids.toArray()));
		
		// Step 2: Querying loopdata to find detectors
		
		MongoCollection<Document> loopcollection = database.getCollection("loopdata2");
		
		int sum = 0;
		Date start = null, end = null, date = null;
		try {
			start = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").parse("09-21-2011 00:00:00");
			end = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").parse("09-22-2011 00:00:00");
			System.out.println("Starttime:"+start+" and Endtime:"+end);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}  
		// FindIterable<Document> loopdata = loopcollection.find(Filters.in("detectorid", tokens));
		FindIterable<Document> loopdata1 = loopcollection.find(Filters
				.and(Filters.in("detectorid", tokens), Filters.gte("starttime", start), 
						Filters.lt("starttime", end)));
		int count = 0;
		
		// Step 3: Adding the volumes
		
		for(Document loop : loopdata1) {
			count++;
			if(loop.get("volume") != null) {
				 String string= loop.get("volume").toString();
				 if(string.equals("") || string.equals(null) || string.equals(" ")) continue;
				 int temp = Integer.parseInt(string);
				sum += temp;
			}
		}
			
		
		System.out.println("No of matching records:"+count);
		System.out.println("Sum of volumes: "+sum);
		Instant finishinstance = Instant.now();
		long timeElapsed = Duration.between(startinstance, finishinstance).toMillis();  //in millis
		System.out.println("time taken:"+timeElapsed);
		// close db connection
		mongo.close();

	}
}
