package org.venice.piazza.servicecontroller;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(locations = "classpath:application.properties", ignoreUnknownFields = false, prefix = "core")
public class CoreServiceProperties {
	
	@NotBlank
	private String uuidservice;
	private String logservice;
	
	@NotNull
	private String discoveryservice;
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
	
	
	public String getDiscoveryservice() {
		return discoveryservice;
	}

	public void setDiscoveryservice(String discoveryservice) {
		this.discoveryservice = discoveryservice;
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

}
