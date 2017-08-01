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
package org.venice.piazza.servicecontroller.data.accessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.common.hibernate.dao.AsyncServiceInstanceDao;
import org.venice.piazza.common.hibernate.dao.ServiceJobDao;
import org.venice.piazza.common.hibernate.dao.service.ServiceDao;
import org.venice.piazza.common.hibernate.entity.ServiceEntity;
import org.venice.piazza.servicecontroller.util.CoreServiceProperties;

import exception.InvalidInputException;
import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.logger.AuditElement;
import model.logger.Severity;
import model.response.Pagination;
import model.response.PiazzaResponse;
import model.response.ServiceListResponse;
import model.service.SearchCriteria;
import model.service.async.AsyncServiceInstance;
import model.service.metadata.Service;
import model.service.taskmanaged.ServiceJob;
import util.PiazzaLogger;

/**
 * Class to store Service information in the Database.
 * 
 * @author mlynum
 * 
 */
@Component
public class DatabaseAccessor {
	@Value("${async.stale.instance.threshold.seconds}")
	private int STALE_INSTANCE_THRESHOLD_SECONDS;
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private CoreServiceProperties coreServiceProperties;
	@Autowired
	private Environment environment;

	@Autowired
	private ServiceDao serviceDao;
	@Autowired
	private ServiceJobDao serviceJobDao;
	@Autowired
	private AsyncServiceInstanceDao asyncServiceInstanceDao;

	private static final Logger LOG = LoggerFactory.getLogger(DatabaseAccessor.class);
	private static final String SERVICE_ID = "serviceId";
	private static final String SERVICE_CTR = "serviceController";
	private static final String JOB_ID = "jobId";
	private static final String STARTED_ON = "startedOn";

	/**
	 * Deletes existing registered service from the Database
	 * 
	 * @param serviceId
	 *            Service id to be deleted
	 * @param softDelete
	 *            If softDelete is true, updates status to OFFLINE instead of deleting.
	 *
	 * @return Message string containing result
	 */
	public String delete(String serviceId, boolean softDelete) {
		String result = null;
		if (softDelete) {
			ServiceEntity serviceEntity = serviceDao.getServiceById(serviceId);
			if (serviceEntity != null) {
				if (serviceEntity.getService().getResourceMetadata() == null) {
					serviceEntity.getService().setResourceMetadata(new ResourceMetadata());
				}
				serviceEntity.getService().getResourceMetadata().setAvailability(ResourceMetadata.STATUS_TYPE.OFFLINE.toString());
				serviceDao.save(serviceEntity);
				result = " service " + serviceId + " was disabled ";
			}
		} else {
			logger.log(String.format("Deleting resource from MongoDB %s", serviceId), Severity.INFORMATIONAL,
					new AuditElement(SERVICE_CTR, "Deleted Service", serviceId));
			serviceDao.deleteServiceByServiceId(serviceId);
			// If any Service Queue exists, also delete that here.
			deleteServiceQueue(serviceId);
			result = " service " + serviceId + " was deleted ";
		}
		return result;
	}

	/**
	 * Store the new service information
	 */
	public String save(Service service) {
		logger.log(String.format("Saving resource in MongoDB %s", service.getServiceId()), Severity.INFORMATIONAL,
				new AuditElement(SERVICE_CTR, "Created Service ", service.getServiceId()));
		serviceDao.save(new ServiceEntity(service));
		return service.getServiceId();
	}

	/**
	 * List all Services. Could be large. Use the paginated version instead.
	 */
	public List<Service> list() {
		Iterable<ServiceEntity> results = serviceDao.getAllAvailableServices();
		List<Service> services = new ArrayList<Service>();
		for (ServiceEntity serviceEntity : results) {
			services.add(serviceEntity.getService());
		}
		return services;
	}

	/**
	 * Get a list of services with pagination
	 */
	public PiazzaResponse getServices(Integer page, Integer perPage, String order, String sortBy, String keyword, String userName) {
		// Execute the appropriate query based on the nullable, optional parameters
		Pagination pagination = new Pagination(null, page, perPage, sortBy, order);
		Page<ServiceEntity> results = null;
		if (((userName != null) && (userName.isEmpty() == false)) && ((keyword != null) && (keyword.isEmpty() == false))) {
			// Both parameters specified
			results = serviceDao.getServiceListForUserAndKeyword(keyword, userName, pagination);
		} else if ((userName != null) && (userName.isEmpty() == false)) {
			// Query by User
			results = serviceDao.getServiceListByUser(userName, pagination);
		} else if ((keyword != null) && (keyword.isEmpty() == false)) {
			// Query by Keyword
			results = serviceDao.getServiceListByKeyword(keyword, pagination);
		} else {
			// Query all Jobs
			results = serviceDao.getServiceList(pagination);
		}

		// Collect the Jobs
		List<Service> services = new ArrayList<Service>();
		for (ServiceEntity serviceEntity : results) {
			services.add(serviceEntity.getService());
		}
		// Set Pagination count
		pagination.setCount(results.getTotalElements());

		// Return the complete List
		return new ServiceListResponse(services, pagination);
	}

	/**
	 * Returns a ResourceMetadata object that matches the specified Id.
	 * 
	 * @param jobId
	 *            Job Id
	 * @return The Job with the specified Id
	 */
	public Service getServiceById(String serviceId) throws ResourceAccessException {
		return null;
		// BasicDBObject query = new BasicDBObject(SERVICE_ID, serviceId);
		// Service service;
		//
		// try {
		// if ((service = getServiceCollection().findOne(query)) == null) {
		// throw new ResourceAccessException(String.format("Service not found : %s", serviceId));
		// }
		// } catch (MongoTimeoutException mte) {
		// LOG.error("MongoDB instance not available", mte);
		// throw new ResourceAccessException(MONGO_NOT_AVAILABLE);
		// }
		//
		// return service;
	}

	/**
	 * Returns a list of ResourceMetadata based on the criteria provided
	 * 
	 * @return List of matching services that match the search criteria
	 */
	public List<Service> search(SearchCriteria criteria) {
		return null;
		// final List<Service> results = new ArrayList<Service>();
		//
		// if (criteria == null) {
		// return results;
		// }
		//
		// LOG.debug("Criteria field=" + criteria.getField());
		// LOG.debug("Criteria field=" + criteria.getPattern());
		//
		// Pattern pattern = Pattern.compile(criteria.getPattern());
		// BasicDBObject query = new BasicDBObject(criteria.getField(), pattern);
		//
		// try {
		//
		// DBCursor<Service> cursor = getServiceCollection().find(query);
		// while (cursor.hasNext()) {
		// results.add(cursor.next());
		// }
		//
		// // Now try to look for the field in the resourceMetadata just to make sure
		// query = new BasicDBObject("resourceMetadata." + criteria.getField(), pattern);
		// cursor = getServiceCollection().find(query);
		//
		// while (cursor.hasNext()) {
		// final Service serviceItem = cursor.next();
		//
		// if (!exists(results, serviceItem.getServiceId())) {
		// results.add(serviceItem);
		// }
		// }
		//
		// return results;
		// } catch (MongoTimeoutException mte) {
		// LOG.error("MongoDB instance not available", mte);
		// throw new ResourceAccessException(MONGO_NOT_AVAILABLE);
		// }
	}

	/**
	 * Checks to see if the result was already found
	 * 
	 * @return true - result is already there false - result has not been found
	 */
	private boolean exists(List<Service> serviceResults, String id) {
		return false;
		// boolean doesExist = false;
		//
		// for (int i = 0; i < serviceResults.size(); i++) {
		// String serviceItemId = serviceResults.get(i).getServiceId();
		// if (serviceItemId.equals(id))
		// doesExist = true;
		// }
		// return doesExist;

	}

	/**
	 * Adds an Asynchronous Service Instance to the Database.
	 * 
	 * @param instance
	 *            The instance
	 */
	public void addAsyncServiceInstance(AsyncServiceInstance instance) {
		// getAsyncServiceInstancesCollection().insert(instance);
	}

	/**
	 * @return Gets all Instances that require a status check.
	 */
	public List<AsyncServiceInstance> getStaleServiceInstances() {
		return null;
		// Get the time to query. Threshold seconds ago, in epoch.
		// long thresholdEpoch = new DateTime().minusSeconds(STALE_INSTANCE_THRESHOLD_SECONDS).getMillis();
		// // Query for all results that are older than the threshold time
		// DBCursor<AsyncServiceInstance> cursor = getAsyncServiceInstancesCollection()
		// .find(DBQuery.lessThan("lastCheckedOn", thresholdEpoch));
		// return cursor.toArray();
	}

	/**
	 * Gets the Async Service Instance for the Piazza Job ID
	 * 
	 * @param jobId
	 *            The piazza Job ID
	 * @return The async service instance
	 */
	public AsyncServiceInstance getInstanceByJobId(String jobId) {
		return null;
		// BasicDBObject query = new BasicDBObject(JOB_ID, jobId);
		// AsyncServiceInstance instance = getAsyncServiceInstancesCollection().findOne(query);
		// return instance;
	}

	/**
	 * Updates an Async Service instance.
	 */
	public void updateAsyncServiceInstance(AsyncServiceInstance instance) {
		// getAsyncServiceInstancesCollection().update(DBQuery.is(JOB_ID, instance.getJobId()), instance);
	}

	/**
	 * Deletes an Async Service Instance by Job ID. This is done when the Service has been processed to completion and
	 * the instance is no longer needed.
	 * 
	 * @param id
	 *            The Job ID of the Async Service Instance.
	 */
	public void deleteAsyncServiceInstance(String jobId) {
		// getAsyncServiceInstancesCollection().remove(DBQuery.is(JOB_ID, jobId));
	}

	/**
	 * Gets a list of all Piazza Services that are registered as a Task-Managed Service.
	 * 
	 * @return List of Task-Managed Services
	 */
	public List<Service> getTaskManagedServices() {
		return null;
		// return getServiceCollection().find().and(DBQuery.is("isTaskManaged", true)).toArray();
	}

	/**
	 * Gets the next Job in the queue for a particular service.
	 * <p>
	 * This method is synchronized because it is incredibly important that we never return the same job twice, avoiding
	 * potential race conditions.
	 * </p>
	 * 
	 * @param serviceId
	 *            The ID of the Service to fetch work for.
	 * @return The next ServiceJob in the Service Queue, if one exists; null if the Queue has no Jobs ready to be
	 *         processed.
	 */
	public synchronized ServiceJob getNextJobInServiceQueue(String serviceId) {
		
		return null;
		// Query for Service Jobs, sort by Queue Time, so that we get the single most stale Job. Ignore Jobs that have
		// already been started. Find the latest.
		// DBCursor<ServiceJob> cursor = getServiceJobCollection(serviceId).find().sort(DBSort.asc("queuedOn"))
		// .and(DBQuery.is(STARTED_ON, null)).limit(1);
		// if (!cursor.hasNext()) {
		// // No available Jobs to be processed.
		// return null;
		// }
		// ServiceJob serviceJob = cursor.next();
		// // Set the current Started Time that this Job was pulled off the queue
		// getServiceJobCollection(serviceId).update(DBQuery.is(JOB_ID, serviceJob.getJobId()),
		// DBUpdate.set(STARTED_ON, new DateTime().getMillis()));
		// // Return the Job
		// return serviceJob;
	}

	/**
	 * Gets the complete list of all Jobs in the Service's Service Queue that have exceeded the length of time for
	 * processing, and are thus considered timed out.
	 * 
	 * @param serviceId
	 *            The ID of the Service whose Queue to check
	 * @return The list of timed out Jobs
	 */
	public List<ServiceJob> getTimedOutServiceJobs(String serviceId) {
		return null;
		// Get the Service details to find out the timeout information
		// Service service = getServiceById(serviceId);
		// Long timeout = service.getTimeout();
		// if (timeout == null) {
		// // If no timeout is specified for the Service, then we can't check for timeouts.
		// return new ArrayList<ServiceJob>();
		// }
		// // The timeout is in seconds. Get the current time and subtract the number of seconds to find the timestamp
		// of a
		// // timed out service.
		// long timeoutEpoch = new DateTime().minusSeconds(timeout.intValue()).getMillis();
		// // Query the database for Jobs whose startedOn field is older than the timeout date.
		// JacksonDBCollection<ServiceJob, String> collection = getServiceJobCollection(serviceId);
		// DBCursor<ServiceJob> jobs = collection.find(DBQuery.exists(STARTED_ON));
		// jobs = jobs.and(DBQuery.lessThan(STARTED_ON, timeoutEpoch));
		// // Return the list
		// return jobs.toArray();
	}

	/**
	 * Gets the Service Job for the specified service with the specified Job ID
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param jobId
	 *            The ID of the Job
	 * @return
	 */
	public ServiceJob getServiceJob(String serviceId, String jobId) {
		return null;
		// BasicDBObject query = new BasicDBObject(JOB_ID, jobId);
		// ServiceJob serviceJob;
		//
		// try {
		// serviceJob = getServiceJobCollection(serviceId).findOne(query);
		// } catch (MongoTimeoutException mte) {
		// String error = "Mongo Instance Not Available.";
		// LOG.error(error, mte);
		// throw new MongoException(error);
		// }
		//
		// return serviceJob;
	}

	/**
	 * Increments the timeout count for the Job ID
	 * 
	 * @param serviceId
	 *            The Service ID containing the Job
	 * @param serviceJob
	 *            The ServiceJob that has timed out
	 */
	public synchronized void incrementServiceJobTimeout(String serviceId, ServiceJob serviceJob) {
		// Increment the failure count.
		// getServiceJobCollection(serviceId).update(DBQuery.is(JOB_ID, serviceJob.getJobId()),
		// DBUpdate.set("timeouts", serviceJob.getTimeouts() + 1));
		// // Delete the previous Started On date, so that it can be picked up again.
		// getServiceJobCollection(serviceId).update(DBQuery.is(JOB_ID, serviceJob.getJobId()),
		// DBUpdate.unset(STARTED_ON));
	}

	/**
	 * Adds a new Service Job reference to the specified Service's Job Queue.
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param serviceJob
	 *            The ServiceJob, describing the ID of the Job
	 */
	public void addJobToServiceQueue(String serviceId, ServiceJob serviceJob) {
		// getServiceJobCollection(serviceId).insert(serviceJob);
	}

	/**
	 * Removes the specified Service Job (by ID) from the Service Queue for the specified Service
	 * 
	 * @param serviceId
	 *            The ID of the service whose Job to remove
	 * @param jobId
	 *            The ID of the Job to remove from the queue
	 */
	public void removeJobFromServiceQueue(String serviceId, String jobId) {
		// DBObject matchJob = new BasicDBObject(JOB_ID, jobId);
		// getServiceJobCollection(serviceId).remove(matchJob);
	}

	/**
	 * Returns a Job that matches the specified Id.
	 * 
	 * @param jobId
	 *            Job Id
	 * @return The Job with the specified Id
	 * @throws InterruptedException
	 */
	public Job getJobById(String jobId) throws ResourceAccessException, InterruptedException {
		return null;
		// BasicDBObject query = new BasicDBObject(JOB_ID, jobId);
		// Job job;
		//
		// try {
		// if ((job = getJobCollection().findOne(query)) == null) {
		// // In case the Job was being updated, or it doesn't exist at this point, try once more. I admit this is
		// // not optimal, but it certainly covers a host of race conditions.
		// Thread.sleep(100);
		// job = getJobCollection().findOne(query);
		// }
		// } catch (MongoTimeoutException mte) {
		// LOG.error(mte.getMessage(), mte);
		// throw new ResourceAccessException(MONGO_NOT_AVAILABLE);
		// }
		//
		// return job;
	}

	/**
	 * Completely deletes a Service Queue for a registered service.
	 * 
	 * @param serviceId
	 *            The ID of the Service whose queue to drop.
	 */
	public void deleteServiceQueue(String serviceId) {
		// getServiceJobCollection(serviceId).drop();
	}

	/**
	 * Determines if the User is able to access the Service Queue for the specified service or not.
	 * 
	 * @param serviceId
	 *            The ID of the Service
	 * @param username
	 *            The user name
	 * @return True if able to access, false if not.
	 */
	public boolean canUserAccessServiceQueue(String serviceId, String username) throws InvalidInputException {
		return false;
		// try {
		// Service service = getServiceById(serviceId);
		// if (service.getTaskAdministrators() != null) {
		// return service.getTaskAdministrators().contains(username);
		// } else {
		// return false;
		// }
		// } catch (ResourceAccessException exception) {
		// LOG.info(String.format("User %s attempted to check Service Queue for non-existent service with ID %s",
		// username, serviceId),
		// exception);
		// throw new InvalidInputException(String.format("Service not found : %s", serviceId));
		// }
	}

	/**
	 * Gets Metadata on the specified Service Queue
	 * 
	 * @param serviceId
	 *            The ID of the service
	 * @return Map containing metadata information
	 */
	public Map<String, Object> getServiceQueueCollectionMetadata(String serviceId) {
		return null;
		// Map<String, Object> map = new HashMap<>();
		// // Get the Length
		// map.put("totalJobCount", getServiceJobCollection(serviceId).find().count());
		// return map;
	}
}