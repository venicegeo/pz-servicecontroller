package org.venice.piazza.servicecontroller.data.mongodb.accessors;


import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mongojack.JacksonDBCollection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.model.Service;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Helper class to interact with and access the Mongo instance.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class MongoAccessor {
	/*@Value("${mongo.host}")
	private String DATABASE_HOST;
	@Value("${mongo.port}")
	private int DATABASE_PORT;
	@Value("${mongo.db.name}")
	private String DATABASE_NAME;
	@Value("${mongo.db.collection.name}")
	private String JOB_COLLECTION_NAME;
	private MongoClient mongoClient;

	public MongoAccessor() {
	}

	@PostConstruct
	private void initialize() {
		try {
			mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		} catch (UnknownHostException exception) {
			System.out.println("Error connecting to MongoDB Instance.");
			exception.printStackTrace();
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
	/*public MongoClient getClient() {
		return mongoClient;
	}*/

	/**
	 * Gets a reference to the MongoDB's Service Collection.
	 * 
	 * @return
	 */
	/*public JacksonDBCollection<Service, String> getServiceCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(JOB_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Service.class, String.class);
	}*/

	/**
	 * Returns a Service instance that matches the specified ID.
	 * 
	 * @param id
	 *            the ID of the service
	 * @return The Service with the specified id
	 */
	/*public Service getServiceById(String id) throws ResourceAccessException {
		BasicDBObject query = new BasicDBObject("serviceId", id);
		Service job = getServiceCollection().findOne(query);
		if (job == null) {
			throw new ResourceAccessException("The Service could not be found");
		}
		return job;
	} */
}