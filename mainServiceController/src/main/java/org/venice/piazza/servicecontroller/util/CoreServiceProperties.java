package org.venice.piazza.servicecontroller.util;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Properties for the ServiceController.  Loaded in from application.properties file.  Properties defined in the 
 * application.properties file will be superseded by values retrieved from the pz-discover service.
 * @author mlynum
 * @version 1.0
 */
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(locations = "classpath:application.properties", ignoreUnknownFields = false, prefix = "core")
public class CoreServiceProperties {
	
	@NotBlank
	private String uuidservice;
	private String logservice;
	@NotNull
	private String discoverapi;
	@NotNull
	private String db;
	@NotNull
	private String kafka;
	@NotNull
	private String uuid;
	@NotNull
	private String logger;
	@NotNull
	private String discoverservice;
	@Value ("${kafka.host}")
	private String kafkaHost;
	@Value ("${kafka.group}")
	private String kafkaGroup;
	@Value ("${kafka.port}")
	private int kafkaPort;
	@Value ("${server.port}")
	private int serverPort;
	@Value ("${mongo.host}")
	private String mongoHost;
	@Value ("${mongo.port}")
	private int mongoPort;
	@Value ("${mongo.db.name}")
	private String mongoDBName;
	@Value ("${mongo.db.collection.name}")
	private String mongoCollectionName;
	@Value("${servicecontroller.appname}")
	private String appname;
	@Value("${servicecontroller.host}")
	private String host;
	@Value("${servicecontroller.port}")
	private String port;
	
	

	public String getKafka() {
		return kafka;
	}

	public void setKafka(String kafka) {
		this.kafka = kafka;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public String getDiscoverapi() {
		return discoverapi;
	}

	public void setDiscoverapi(String discoverapi) {
		this.discoverapi = discoverapi;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getAppname() {
		return appname;
	}

	public void setAppname(String appname) {
		this.appname = appname;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getDiscoverservice() {
		return discoverservice;
	}

	public void setDiscoverservice(String discoverservice) {
		this.discoverservice = discoverservice;
	}	
	public String getKafkaHost() {
		return kafkaHost;
	}

	public void setKafkaHost(String kafkaHost) {
		this.kafkaHost = kafkaHost;
	}

	public String getKafkaGroup() {
		return kafkaGroup;
	}

	public void setKafkaGroup(String kafkaGroup) {
		this.kafkaGroup = kafkaGroup;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public String getMongoHost() {
		return mongoHost;
	}

	public void setMongoHost(String mongoHost) {
		this.mongoHost = mongoHost;
	}

	public int getMongoPort() {
		return mongoPort;
	}

	public void setMongoPort(int mongoPort) {
		this.mongoPort = mongoPort;
	}

	public String getMongoDBName() {
		return mongoDBName;
	}

	public void setMongoDBName(String mongoDBName) {
		this.mongoDBName = mongoDBName;
	}

	public String getMongoCollectionName() {
		return mongoCollectionName;
	}

	public void setMongoCollectionName(String mongoCollectionName) {
		this.mongoCollectionName = mongoCollectionName;
	}
	
	public int getKafkaPort() {
		return kafkaPort;
	}

	public void setKafkaPort(int kafkaPort) {
		this.kafkaPort = kafkaPort;
	}

	public String getLogservice() {
		return logservice;
	}

	public void setLogservice(String logservice) {
		this.logservice = logservice;
	}

	public String getUuidservice() {
		return uuidservice;
	}

	public void setUuidservice(String uuidservice) {
		this.uuidservice = uuidservice;
	}
	
	@Bean
	public CoreInitDestroy coreInitDestroy() {
		return new CoreInitDestroy();
	}

}
