package org.venice.piazza.servicecontroller.data.mongodb.accessors;
// TODO add License

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.log4j.Logger;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import model.job.metadata.ResourceMetadata;

/**
 * Class to store service information in MongoDB.  
 * 
 * @author mlynum
 * 
 */
// TODO  FUTURE See a better way to abstract out MongoDB
// TODO  FUTURE See a way to store service controller internals 
@Component
public class MongoAccessor {
	@Value("${mongo.host}")
	private String DATABASE_HOST;
	@Value("${mongo.port}")
	private int DATABASE_PORT;
	@Value("${mongo.db.name}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.name}")
	private String SERVICE_COLLECTION_NAME;
	private MongoClient mongoClient;
	
	private final static Logger LOGGER = Logger.getLogger(MongoAccessor.class);

	public MongoAccessor() {
	}

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException ex) {
			LOGGER.error(ex.getMessage());
			ex.printStackTrace();
		}
	}

	@PreDestroy
	private void close() {
		mongoClient.close();
	}

	/**
	 * Gets a reference to the MongoDB Client Object.
	 * 
	 * @return
	 */
	public MongoClient getClient() {
		return mongoClient;
	}
	
	/**
	 * Store the new service information
	 */
	public String save(ResourceMetadata metadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
			
			JacksonDBCollection<ResourceMetadata, String> coll = JacksonDBCollection.wrap(collection, ResourceMetadata.class,
			        String.class);
			
			WriteResult<ResourceMetadata, String> writeResult = coll.insert(metadata);
			result = writeResult.getSavedId();
			
		} catch (MongoException ex) {
			LOGGER.error(ex);
			LOGGER.error(ex.getMessage());
			
		}
			
		return result;
	}

}