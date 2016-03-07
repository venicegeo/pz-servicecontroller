/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller.data.mongodb.accessors;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.DBQuery.Query;
import org.mongojack.DBUpdate;
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

import model.job.metadata.ResourceMetadata;
import model.job.metadata.Service;
import model.service.SearchCriteria;

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
	private String SERVICE_COLLECTION_NAME;
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
		SERVICE_COLLECTION_NAME = coreServiceProperties.getMongoCollectionName();
		LOGGER.debug("====================================================");
		LOGGER.debug("DATABASE_HOST=" + DATABASE_HOST);
		LOGGER.debug("DATABASE_PORT=" + DATABASE_PORT);
		LOGGER.debug("DATABASE_NAME=" + DATABASE_NAME);
		LOGGER.debug("SERVICE_COLLECTION_NAME=" + SERVICE_COLLECTION_NAME);
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
	public String update(Service sMetadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
			
			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class,
			        String.class);
			
			Query query = DBQuery.is("id",sMetadata.getId());
			
			WriteResult<Service, String> writeResult = coll.update(query,sMetadata);
			// Return the id that was used
			return sMetadata.getId();
			
		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			ex.printStackTrace();
			
		}
			
		return result;
	}
	
	
	/**
	 * deleteservice 
	 */
	public String delete(String serviceId) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
			
			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class,
			        String.class);
			
			
			Query query = DBQuery.is("id",serviceId);
			WriteResult<Service, String> writeResult =
					coll.update(query,DBUpdate.set("rmetadata.availability", "OUT OF SERVICE"));
			int recordsChanged = writeResult.getN();
			// Return the id that was used
			if (recordsChanged == 1) {
				result = " service " + serviceId + " deleted ";
			}
			else {
				result = " service " + serviceId + " NOT deleted ";
			}
			
			return result;
			
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
	public String save(Service sMetadata) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
			
			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class,
			        String.class);
			
			WriteResult<Service, String> writeResult = coll.insert(sMetadata);
			// Return the id that was used
			return sMetadata.getId();
			
		} catch (MongoException ex) {
			LOGGER.debug(ex.toString());
			LOGGER.error(ex.getMessage());
			ex.printStackTrace();
			
		}
			
		return result;
	}
	
	/**
	 * List services
	 */
	public List<Service> list() {
		ArrayList<Service> result = new ArrayList<Service>();
		try {
			
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
			
			JacksonDBCollection<Service, String> coll = JacksonDBCollection.wrap(collection, Service.class,
			        String.class);
			
			DBCursor<Service> metadataCursor = 
					coll.find(DBQuery.notEquals("availability", "OUT OF SERVICE"));
			while (metadataCursor.hasNext()) {
				result.add(metadataCursor.next());
			}
			// Return the id that was used
			return result;
			
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
	public JacksonDBCollection<Service, String> getServiceCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(SERVICE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, Service.class, String.class);
	}

	
	/**
	 * Returns a ResourceMetadata object that matches the specified ID.
	 * 
	 * @param jobId
	 *            Job ID
	 * @return The Job with the specified ID
	 */
	public Service getServiceById(String serviceId) throws ResourceAccessException {
	
		
		BasicDBObject query = new BasicDBObject("id", serviceId);
		Service service;

		try {
			if ((service = getServiceCollection().findOne(query)) == null) {
				throw new ResourceAccessException("Service not found.");
			}			
		} catch( MongoTimeoutException mte) {
			throw new ResourceAccessException("MongoDB instance not available.");
		}

		return service;
	}
	
	/**
	 * Returns a list of ResourceMetadata based on the criteria provided
	 */
	public List <Service> search(SearchCriteria criteria) {
		List <Service> results =  new ArrayList<Service>();
		if (criteria != null) {
			
	     LOGGER.debug("Criteria field=" + criteria.getField());
	     LOGGER.debug("Criteria field=" + criteria.getPattern());


		Pattern pattern = Pattern.compile(criteria.pattern);
		BasicDBObject query = new BasicDBObject(criteria.field, pattern);

			try {
				
				DBCursor<Service> cursor = getServiceCollection().find(query);		
				while (cursor.hasNext()) {
					results.add(cursor.next());
				}
			} catch( MongoTimeoutException mte) {
				throw new ResourceAccessException("MongoDB instance not available.");
			}
		}

		return results;
	}

}