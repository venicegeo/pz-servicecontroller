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

import model.job.metadata.ResourceMetadata;

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
import org.venice.piazza.servicecontroller.data.model.SearchCriteria;
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
	 * deleteservice 
	 */
	public String delete(String resourceId) {
		String result = "";
		try {
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
			
			JacksonDBCollection<ResourceMetadata, String> coll = JacksonDBCollection.wrap(collection, ResourceMetadata.class,
			        String.class);
			
			
			Query query = DBQuery.is("id",resourceId);
			WriteResult<ResourceMetadata, String> writeResult =
					coll.update(query,DBUpdate.set("availability", "OUT OF SERVICE"));
			int recordsChanged = writeResult.getN();
			// Return the id that was used
			if (recordsChanged == 1) {
				result = " resource " + resourceId + " deleted ";
			}
			else {
				result = " resource " + resourceId + " NOT deleted ";
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
	 * List services
	 */
	public List<ResourceMetadata> list() {
		ArrayList<ResourceMetadata> result = new ArrayList<ResourceMetadata>();
		try {
			
			DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
			
			JacksonDBCollection<ResourceMetadata, String> coll = JacksonDBCollection.wrap(collection, ResourceMetadata.class,
			        String.class);
			
			DBCursor<ResourceMetadata> metadataCursor = 
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
	public JacksonDBCollection<ResourceMetadata, String> getResourceCollection() {
		// MongoJack does not support the latest Mongo API yet. TODO: Check if
		// they plan to.
		DBCollection collection = mongoClient.getDB(DATABASE_NAME).getCollection(RESOURCE_COLLECTION_NAME);
		return JacksonDBCollection.wrap(collection, ResourceMetadata.class, String.class);
	}

	
	/**
	 * Returns a ResourceMetadata object that matches the specified ID.
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
	
	/**
	 * Returns a list of ResourceMetadata based on the criteria provided
	 */
	public List <ResourceMetadata> search(SearchCriteria criteria) {
		List <ResourceMetadata> results =  new ArrayList<ResourceMetadata>();

		if (criteria != null) {
		Pattern pattern = Pattern.compile(criteria.pattern);
		BasicDBObject query = new BasicDBObject(criteria.field, pattern);

			try {
				DBCursor<ResourceMetadata> cursor = getResourceCollection().find(query);		
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