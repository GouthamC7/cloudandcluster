package mongo;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class Cleaning {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String uriString = "mongodb://35.239.16.148:"+27017;

		// Connecting to mongo db

		MongoClientURI uri = new MongoClientURI(uriString);
		MongoClient mongo = new MongoClient(uri);
		MongoDatabase database = mongo.getDatabase("config");
		Instant startinstance = Instant.now();
		System.out.println("connected");
		MongoCollection<Document> loopcollection = database.getCollection("loopdata2");
		FindIterable<Document> stations = loopcollection.find();
		//loopcollection.deleteMany(Filters.or(Filters.eq("speed", "0"), Filters.eq("speed", "")));
		int count = 0;
		for(Document station : stations) {
			count++;
	}
		System.out.println(count);
		System.out.println("Successful");
		mongo.close();
	}

}
