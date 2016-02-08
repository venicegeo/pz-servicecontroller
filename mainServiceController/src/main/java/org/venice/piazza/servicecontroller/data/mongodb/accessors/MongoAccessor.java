package org.venice.piazza.servicecontroller.data.mongodb.accessors;
// TODO add License

import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import model.job.metadata.ResourceMetadata;

import org.mongojack.DBQuery;
import org.mongojack.DBQuery.Query;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;

/**
 * Class to store service information in MongoDB.  
 * 
 * @author mlynum
 * 
 */
// TODO  FUTURE See a better way to abstract out MongoDB
// TODO  FUTURE See a way to store service controller internals 
@Component
@DependsOn("coreInitDestroy")
public class MongoAccessor {
	private String DATABASE_HOST;
	private int DATABASE_PORT;
	private String DATABASE_NAME;
	private String RESOURCE_COLLECTION_NAME;
	private MongoClient mongoClient;
	
	@Autowired
	private CoreServiceProperties coreServiceProperties;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(MongoAccessor.class);

	public MongoAccessor() {
	}

	@PostConstruct
	private void initialize() {
		// Initialize the MongoDB 
		DATABASE_HOST = coreServiceProperties.getMongoHost();
		DATABASE_PORT = coreServiceProperties.getMongoPort();
		DATABASE_NAME = coreServiceProperties.getMongoDBName();
		RESOURCE_COLLECTION_NAME = coreServiceProperties.getMongoCollectionName();
		LOGGER.debug("====================================================");
		LOGGER.debug("DATABASE_HOST=" + DATABASE_HOST);
		LOGGER.debug("DATABASE_PORT=" + DATABASE_PORT);
		LOGGER.debug("DATABASE_NAME=" + DATABASE_NAME);
		LOGGER.debug("RESOURCE_COLLECTION_NAME=" + RESOURCE_COLLECTION_NAME);
		LOGGER.debug("====================================================");

		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException ex) {
			LOGGER.error(ex.getMessage());
			LOGGER.debug(ex.toString());
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
	 * updateservice information
	 */
	public String update(ResourceMetadata metadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
			
			JacksonDBCollection<ResourceMetadata, String> coll = JacksonDBCollection.wrap(collection, ResourceMetadata.class,
			        String.class);
			
			Query query = DBQuery.is("id",metadata.id);
			
			WriteResult<ResourceMetadata, String> writeResult = coll.update(query,metadata);
			// Return the id that was used
			return metadata.id;
			
		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			ex.printStackTrace();
			
		}
			
		return result;
	}
	

	
	/**
	 * Store the new service information
	 */
	public String save(ResourceMetadata metadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
			
			JacksonDBCollection<ResourceMetadata, String> coll = JacksonDBCollection.wrap(collection, ResourceMetadata.class,
			        String.class);
			
			WriteResult<ResourceMetadata, String> writeResult = coll.insert(metadata);
			// Return the id that was used
			return metadata.id;
			
		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			ex.printStackTrace();
			
		}
			
		return result;
	}
	

	/**
	 * Gets a reference to the MongoDB's Job Collection.
	 * 
	 * @return
	 */
	public JacksonDBCollection<ResourceMetadata, String> getResourceCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, ResourceMetadata.class, String.class);
	}

	
	/**
	 * Returns a Job that matches the specified ID.
	 * 
	 * @param jobId
	 *            Job ID
	 * @return The Job with the specified ID
	 */
	public ResourceMetadata getResourceById(String resourceId) throws ResourceAccessException {
	
		
		BasicDBObject query = new BasicDBObject("id", resourceId);
		ResourceMetadata resource;

		try {
			if ((resource = getResourceCollection().findOne(query)) == null) {
				throw new ResourceAccessException("ResourceMetadata not found.");
			}			
		} catch( MongoTimeoutException mte) {
			throw new ResourceAccessException("MongoDB instance not available.");
		}

		return resource;
	}

}