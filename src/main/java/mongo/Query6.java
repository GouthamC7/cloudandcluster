package mongo;

import java.time.Instant;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Updates.*;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.UpdateOptions;


// Update: Change the milepost of the Foster NB station to 22.6.

public class Query6 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String uriString = "mongodb://35.192.174.210:"+27017;

		// Connecting to mongo

		MongoClientURI uri = new MongoClientURI(uriString);
		MongoClient mongo = new MongoClient(uri);
		MongoDatabase database = mongo.getDatabase("config");
		Instant startinstance = Instant.now();
		MongoCollection<Document> collection = database.getCollection("stationcollection2");
		
		// updating document
		
		collection.updateOne(Filters.in("locationtext", "Foster NB"), set("milepost", "22.6"));
	}

}
