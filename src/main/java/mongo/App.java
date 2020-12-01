package mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

public class App {
	public static void main(String[] args) {
		/**** Read from CSV ****/
		String highwayFile = "C:/Users/podil/Downloads/highways.csv";
		String stationsFile = "C:/Users/podil/Downloads/freeway_stations.csv";
		String detectorsFile = "C:/Users/podil/Downloads/freeway_detectors.csv";
		String loopDataFile = "C:/Users/podil/Downloads/freeway_loopdata.csv";
		BufferedReader brh = null;
		BufferedReader brs = null;
		BufferedReader brd = null;
		BufferedReader br = null;
		String hl = "";
		String sl = "";
		String dl = "";
		String loop = "";
		String cvsSplitBy = ",";
		
		String uriString = "mongodb://35.192.174.210:"+27017;

		// Connecting to mongo db

		MongoClientURI uri = new MongoClientURI(uriString);
		MongoClient mongo = new MongoClient(uri);
		MongoDatabase database = mongo.getDatabase("config");
		MongoIterable<String> mongoIterable = database.listCollectionNames();
		MongoCursor<String> iterator = mongoIterable.iterator();
		MongoCredential credential;
		System.out.println("Connected to the database successfully");
		MongoCollection<Document> collection = database.getCollection("stationcollection3");

		// read CSV data
		try {
			Document highway_document;
			Document station_document = null;
			Document detector_document;
			Document loop_document;
			
			brs = new BufferedReader(new FileReader(stationsFile));
			while ((sl = brs.readLine()) != null) {
				String[] station = sl.split(cvsSplitBy);
				String stationid = station[0];
				String highwayid = station[1];
				//System.out.println(station[8] + "---" +station[9] + "---");
				if (station[1] != null && !station[1].isEmpty()) {
					station_document = new Document("stationid", station[0]).append("highwayid", station[1])
							.append("milepost", station[2]).append("locationtext", station[3])
							.append("upstream", station[4]).append("downstream", station[5])
							.append("stationclass", station[6]).append("numberlanes", station[7])
							.append("latlon", station[8]).append("length", station[9]);

					brd = new BufferedReader(new FileReader(detectorsFile));
					List<Document> detectordocuments = new ArrayList<Document>();
					while ((dl = brd.readLine()) != null) {
						String[] detector = dl.split(cvsSplitBy);
						if (detector[6] != null && !detector[6].isEmpty()
								&& detector[6].equalsIgnoreCase(stationid)) {
							detector_document = new Document("detectorid", detector[0]).append("detectorclass", detector[4])
									.append("lanenumber", detector[5]);
							detectordocuments.add(detector_document);
						}
					}
					station_document.append("detectors", detectordocuments);
					brh = new BufferedReader(new FileReader(highwayFile));
					while ((hl = brh.readLine()) != null) {
						String[] highway = hl.split(cvsSplitBy);
						if(highway[0]!=null && !highway[0].isEmpty() && highway[0].equalsIgnoreCase(highwayid)) {
							station_document.append("shortdirection", highway[1])
								.append("direction", highway[2]).append("highwayname", highway[3]);
						}
				}
					

			}
				// Inserting documents into the collection
				collection.insertOne(station_document);
			}
			/*MongoCollection<Document> loopCollection = database.getCollection("loopdata");
			br = new BufferedReader(new FileReader(loopDataFile));
			Date startDate = new Date();
			SimpleDateFormat sd = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
			while ((loop = br.readLine()) != null) {
				String[] loop_record = loop.split(cvsSplitBy);
				if(loop_record[1].equalsIgnoreCase("starttime")) {
					continue;
				}
				try {
					//System.out.println(loop_record[1]);
					startDate = sd.parse(loop_record[1].substring(0,18));
//					System.out.println(startDate.getTime());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				loop_document = new Document("detectorid", loop_record[0] != null ? Integer.parseInt(loop_record[0]) : null)
						.append("starttime", startDate.getTime())
						.append("volume", loop_record[2].equals("") ? null : Integer.parseInt(loop_record[2]))
						.append("speed", loop_record[3].equals("") ? null : Integer.parseInt(loop_record[3]))
						.append("occupany", loop_record[4].equals("") ? null : Integer.parseInt(loop_record[4]))
						.append("status", loop_record[5].equals("") ? "" : Integer.parseInt(loop_record[5]))
						.append("dqflags", loop_record[6].equals("") ? "" : Integer.parseInt(loop_record[6]));
				loopCollection.insertOne(loop_document);
			}*/
			
			
			System.out.println("Execution finished!!");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (brh != null) {
				try {
					brh.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (brs != null) {
				try {
					brs.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (brd != null) {
				try {
					brd.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
