package org.venice.piazza.servicecontroller.data.mongodb.accessors;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import model.data.type.LiteralDataType;
import model.data.type.TextDataType;
import model.job.metadata.ResourceMetadata;
import model.service.metadata.Format;
import model.service.metadata.MetadataType;
import model.service.metadata.ParamDataItem;
import model.service.metadata.Service;




public class InsertServiceTest {
	private Properties props = new Properties();
	private String DATABASE_HOST;
	private int DATABASE_PORT;
	private String DATABASE_NAME;
	private String RESOURCE_COLLECTION_NAME;
	private MongoClient mongoClient;
	private Service service;
	
	@Before
	public void setUp() {
		InputStream is = this.getClass().getResourceAsStream("/application.properties");
		try {
		  props.load(is);
		  DATABASE_HOST = props.getProperty("mongo.host");
		  DATABASE_PORT = Integer.parseInt(props.getProperty("mongo.port"));
		  DATABASE_NAME = props.getProperty("mongo.db.name");
		  RESOURCE_COLLECTION_NAME = props.getProperty("mongo.db.collection.name");
		  mongoClient = new MongoClient(DATABASE_HOST, DATABASE_PORT);
		}
		
		catch (IOException e) {
			
		}
		service = new Service();
		service.setName("Feathers");
		ResourceMetadata rm = new ResourceMetadata();
		rm.availability = "NOW";
		rm.description = "Fluffing";
		rm.url = "http://parrotTrust.org";
		rm.method = "GET";
		rm.credentialsRequired = false;
		rm.clientCertRequired = false;
		rm.name = "Feathers";
		service.setResourceMetadata(rm);
		ParamDataItem input1 = new ParamDataItem();
		TextDataType dataType1 = new TextDataType();
		input1.setDataType(dataType1);
		input1.setMaxOccurs(1);
		input1.setMinOccurs(1);
		input1.setName("geom");
		Format format1 = new Format();
		format1.setMimeType("text/xml; subtype=gml/3.1.1");
		List<Format> formats1 = new ArrayList<Format>();
		formats1.add(format1);
		input1.setFormats(formats1);
		MetadataType metadata = new MetadataType();
		metadata.setTitle("geom");
		metadata.setAbout("Input Geometry");
		input1.setMetadata(metadata);
		ParamDataItem input2 = new ParamDataItem();
		LiteralDataType dataType2 = new LiteralDataType();
		input2.setDataType(dataType2);
		input2.setMaxOccurs(1);
		input2.setMinOccurs(1);
		input2.setName("distance");
		Format format2 = new Format();
		format2.setMimeType("text/plain");
		List<Format> formats2 = new ArrayList<Format>();
		formats2.add(format2);
		input2.setFormats(formats2);
		MetadataType metadata2 = new MetadataType();
		metadata2.setTitle("distance");
		metadata.setAbout("Distance to buffer the input geometry, in the units of the geometry");
		input2.setMetadata(metadata2);
		List<ParamDataItem> inputs = new ArrayList<ParamDataItem>();
		inputs.add(input1);
		inputs.add(input2);
		service.setInputs(inputs);
		
		ParamDataItem output1 = new ParamDataItem();
		TextDataType dataType3 = new TextDataType();
		output1.setDataType(dataType3);
		output1.setMaxOccurs(1);
		output1.setMinOccurs(1);
		output1.setName("geom");
		
		output1.setFormats(formats1);
		MetadataType outMetadata = new MetadataType();
		metadata.setTitle("outgeom");
		metadata.setAbout("Output Geometry");
		output1.setMetadata(metadata);
		List<ParamDataItem> outputs = new ArrayList<ParamDataItem>();
		outputs.add(output1);
		service.setOutputs(outputs);
		
		
			
	}

	private  DBCollection getCollection() {
		 
		  DB db = mongoClient.getDB( DATABASE_NAME );
		  DBCollection dbCollection = db.getCollection(RESOURCE_COLLECTION_NAME);
		  return dbCollection;
		 }
	@Test
	public void test() {
		DBCollection dbCollection = getCollection();
		JacksonDBCollection<Service,String> coll = JacksonDBCollection.wrap(dbCollection, Service.class,String.class);
		coll.insert(service);
		List<Service> services = coll.find(DBQuery.is("resourceMetadata.description", "Fluffing")).toArray();
		System.out.println("");;
	}

}
