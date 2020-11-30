package mongo;


import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

//  Count low speeds and high speeds: Find the number of speeds < 5 mph and  > 80 mph in the data set.

public class Query1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String uriString = "mongodb://35.188.82.31:"+27017;

		// Connecting to mongo db

		MongoClientURI uri = new MongoClientURI(uriString);
		MongoClient mongo = new MongoClient(uri);
		MongoDatabase database = mongo.getDatabase("config");
		MongoCollection<Document> loopcollection = database.getCollection("loopdata2");
		// Filtering speeds greater than 80 and less than 5
		long result = loopcollection.countDocuments(Filters.or(Filters.gt("speed", "80"), Filters.lt("speed", "5")));
		System.out.println("The number of speeds > 80 and < 5 in the data set:"+result);
		mongo.close();
	}

}