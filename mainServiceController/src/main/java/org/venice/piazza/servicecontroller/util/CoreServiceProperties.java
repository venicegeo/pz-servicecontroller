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
package org.venice.piazza.servicecontroller.util;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Properties for the ServiceController.  Loaded in from application.properties file.  Properties defined in the 
 * application.properties file will be superseded by values retrieved from the pz-discover service.
 * @author mlynum
 * @version 1.0
 */
@Configuration
@EnableConfigurationProperties
@ComponentScan({ "MY_NAMESPACE, util" })
@ConfigurationProperties(locations = "classpath:application.properties", ignoreUnknownFields = false, prefix = "core")
public class CoreServiceProperties {
	
	
	@Value("${pz.uuid.url}")
	private String uuidservicehost;
	private String logservice;
	private String logservicehost;
	private String appname;
	
	@NotNull
	private String discoverapi;
	@NotNull
	private String db;
	@NotNull
	private String kafka;
	@NotNull
	private String uuid;
	@Value("${pz.logger.url}")
	private String logger;
	@NotNull
	private String discoverservice;
	@Value ("${vcap.services.pz-kafka.credentials.host}")
	private String kafkaHost;
	@Value ("${kafka.group}")
	private String kafkaGroup;
	@Value ("${server.port}")
	private int serverPort;
	@Value ("${vcap.services.pz-mongodb.credentials.uri}")
	private String mongoHost;
	@Value ("${vcap.services.pz-mongodb.credentials.database}")
	private String mongoDBName;
	@Value ("${mongo.db.collection.name}")
	private String mongoCollectionName;
	@Value("${servicecontroller.host}")
	private String host;
	@Value("${servicecontroller.port}")
	private String port;
	@Value("${pz.search.protocol}")
	private String pzSearchProtocol;
	@Value("${pz.search.url}")
	private String pzSearchUrl;
	@Value("${pz.servicemetadata.ingest.url}")
	private String pzServicemetadataIngestUrl;
	@Value("${pz.servicemetadata.update.url}")
	private String pzServicemetadataUpdateUrl;
	
	
	
	public String getPzSearchProtocol() {
		return pzSearchProtocol;
	}

	public void setPzSearchProtocol(String pzSearchProtocol) {
		this.pzSearchProtocol = pzSearchProtocol;
	}

	public String getPzSearchUrl() {
		return pzSearchUrl;
	}

	public void setPzSearchUrl(String pzSearchUrl) {
		this.pzSearchUrl = pzSearchUrl;
	}

	public String getPzServicemetadataIngestUrl() {
		return pzServicemetadataIngestUrl;
	}

	public void setPzServicemetadataIngestUrl(String pzServicemetadataIngestUrl) {
		this.pzServicemetadataIngestUrl = pzServicemetadataIngestUrl;
	}

	public String getPzServicemetadataUpdateUrl() {
		return pzServicemetadataUpdateUrl;
	}

	public void setPzServicemetadataUpdateUrl(String pzServicemetadataUpdateUrl) {
		this.pzServicemetadataUpdateUrl = pzServicemetadataUpdateUrl;
	}

	@Value ("${space}")
	private String space;

	public String getUuidservicehost() {
		return uuidservicehost;
	}

	public void setUuidservicehost(String uuidservicehost) {
		this.uuidservicehost = uuidservicehost;
	}

	public String getLogservicehost() {
		return logservicehost;
	}

	public void setLogservicehost(String logservicehost) {
		this.logservicehost = logservicehost;
	}

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
	

	public String getLogservice() {
		return logservice;
	}

	public void setLogservice(String logservice) {
		this.logservice = logservice;
	}


	
	public String getSpace() {
		return space;
	}

	public void setSpace(String space) {
		this.space = space;
	}

	@Bean
	public CoreInitDestroy coreInitDestroy() {
		return new CoreInitDestroy();
	}

}
