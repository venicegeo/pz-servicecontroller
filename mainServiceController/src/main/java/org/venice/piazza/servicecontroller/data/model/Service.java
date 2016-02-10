package org.venice.piazza.servicecontroller.data.model;

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


import java.util.List;

import model.job.Job;

import org.joda.time.DateTime;
import org.springframework.data.annotation.Id;

/**
 * Serves as a data model for Service instances.
 * 
 * @author mlynum
 * @date Dec 16, 2015
 *
 **/
public class Service {
	@Id
	private Long id;
	
	private String availability;
	private String classificationType;
	private Long currentJobId;	
	private String description;
	private Boolean enabled;
	private String name;
	private String networkAvailable;
	private List <Param> params;
	private String poc;
	private Boolean published;
	private List <SecAttributes> security;
	private String serviceQoS;
	private String tags;
	private String technicalPOC;
	private DateTime termDate;
	private String url;
	private String version;
	
	
	public Service () {
		
	}
	/**
	 * Constructor of Service with required name
	 * @param name
	 */
	public Service(String name) {
		this.name = name;
	}
	/**
	 * @return the id
	 */
	public Long getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(Long id) {
		this.id = id;
	}
	/**
	 * @return the availability
	 */
	public String getAvailability() {
		return availability;
	}
	/**
	 * @param availability the availability to set
	 */
	public void setAvailability(String availability) {
		this.availability = availability;
	}
	/**
	 * @return the classificationType
	 */
	public String getClassificationType() {
		return classificationType;
	}
	/**
	 * @param classificationType the classificationType to set
	 */
	public void setClassificationType(String classificationType) {
		this.classificationType = classificationType;
	}
	
	public Long getCurrentJobId() {
		return currentJobId;
	}
	public void setCurrentJobId(Long currentJobId) {
		this.currentJobId = currentJobId;
	}

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return the enabled
	 */
	public Boolean getEnabled() {
		return enabled;
	}
	/**
	 * @param enabled the enabled to set
	 */
	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}
	/**
	 * @return the networkAvailable
	 */
	public String getNetworkAvailable() {
		return networkAvailable;
	}
	/**
	 * @param networkAvailable the networkAvailable to set
	 */
	public void setNetworkAvailable(String networkAvailable) {
		this.networkAvailable = networkAvailable;
	}
	/**
	 * @return the params
	 */
	public List<Param> getParams() {
		return params;
	}
	/**
	 * @param params the params to set
	 */
	public void setParams(List<Param> params) {
		this.params = params;
	}
	/**
	 * @return the poc
	 */
	public String getPoc() {
		return poc;
	}
	/**
	 * @param poc the poc to set
	 */
	public void setPoc(String poc) {
		this.poc = poc;
	}
	/**
	 * @return the published
	 */
	public Boolean getPublished() {
		return published;
	}
	/**
	 * @param published the published to set
	 */
	public void setPublished(Boolean published) {
		this.published = published;
	}
	/**
	 * @return the security
	 */
	public List<SecAttributes> getSecurity() {
		return security;
	}
	/**
	 * @param security the security to set
	 */
	public void setSecurity(List<SecAttributes> security) {
		this.security = security;
	}
	/**
	 * @return the serviceQoS
	 */
	public String getServiceQoS() {
		return serviceQoS;
	}
	/**
	 * @param serviceQoS the serviceQoS to set
	 */
	public void setServiceQoS(String serviceQoS) {
		this.serviceQoS = serviceQoS;
	}
	/**
	 * @return the tags
	 */
	public String getTags() {
		return tags;
	}
	/**
	 * @param tags the tags to set
	 */
	public void setTags(String tags) {
		this.tags = tags;
	}
	/**
	 * @return the technicalPOC
	 */
	public String getTechnicalPOC() {
		return technicalPOC;
	}
	/**
	 * @param technicalPOC the technicalPOC to set
	 */
	public void setTechnicalPOC(String technicalPOC) {
		this.technicalPOC = technicalPOC;
	}
	/**
	 * @return the termDate
	 */
	public DateTime getTermDate() {
		return termDate;
	}
	/**
	 * @param termDate the termDate to set
	 */
	public void setTermDate(DateTime termDate) {
		this.termDate = termDate;
	}
	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}
	/**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}
	/**
	 * @param version the version to set
	 */
	public void setVersion(String version) {
		this.version = version;
	}
	
	@Override
	public String toString () {
		return String.format("Service [id=%s, name=%s]", id, name);
	}

	

}