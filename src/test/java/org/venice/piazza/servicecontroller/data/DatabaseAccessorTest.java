package org.venice.piazza.servicecontroller.data;

import exception.InvalidInputException;
import model.job.Job;
import model.job.metadata.ResourceMetadata;
import model.response.Pagination;
import model.response.PiazzaResponse;
import model.service.async.AsyncServiceInstance;
import model.service.metadata.Service;
import model.service.taskmanaged.ServiceJob;
import org.assertj.core.api.Fail;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.Page;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.common.hibernate.dao.AsyncServiceInstanceDao;
import org.venice.piazza.common.hibernate.dao.ServiceJobDao;
import org.venice.piazza.common.hibernate.dao.job.JobDao;
import org.venice.piazza.common.hibernate.dao.service.ServiceDao;
import org.venice.piazza.common.hibernate.entity.AsyncServiceInstanceEntity;
import org.venice.piazza.common.hibernate.entity.JobEntity;
import org.venice.piazza.common.hibernate.entity.ServiceEntity;
import org.venice.piazza.common.hibernate.entity.ServiceJobEntity;
import org.venice.piazza.servicecontroller.data.accessor.DatabaseAccessor;
import util.PiazzaLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DatabaseAccessorTest {

    private ResourceMetadata metadata;
    private Service service;
    private ServiceEntity serviceEntity;

    private Job job;
    private JobEntity jobEntity;
    private AsyncServiceInstance asyncServiceInstance;
    private AsyncServiceInstanceEntity asyncServiceInstanceEntity;

    @Mock
    private PiazzaLogger logger;
    @Mock
    private JobDao jobDao;
    @Mock
    ServiceDao serviceDao;
    @Mock
    ServiceJobDao serviceJobDao;
    @Mock
    AsyncServiceInstanceDao asyncServiceInstanceDao;

    @InjectMocks
    private DatabaseAccessor accessor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.metadata = new ResourceMetadata();
        this.service = new Service();
        this.service.setServiceId("my_service_id");
        this.service.setResourceMetadata(this.metadata);
        this.serviceEntity = new ServiceEntity(this.service);
        Mockito.when(this.serviceDao.getServiceById(this.serviceEntity.getService().getServiceId())).thenReturn(this.serviceEntity);

        this.job = new Job();
        this.job.setJobId("my_job_id");
        this.jobEntity = new JobEntity(this.job);
        this.asyncServiceInstance = new AsyncServiceInstance();
        this.asyncServiceInstance.setJobId(this.job.getJobId());
        this.asyncServiceInstanceEntity = new AsyncServiceInstanceEntity(this.asyncServiceInstance);
        Mockito.when(this.jobDao.getJobByJobId(this.jobEntity.getJob().getJobId())).thenReturn(this.jobEntity);
        Mockito.when(this.asyncServiceInstanceDao.getInstanceByJobId(this.asyncServiceInstance.getJobId())).thenReturn(this.asyncServiceInstanceEntity);

    }

    @Test
    public void testDelete() {
        this.accessor.delete(this.serviceEntity.getService().getServiceId(), false);
        this.accessor.delete(this.serviceEntity.getService().getServiceId(), true);
        System.out.println();
    }

    @Test
    public void testSave() {
        Service myService = new Service();
        myService.setServiceId("my_service_id");

        this.accessor.save(myService);
    }

    @Test
    public void testUpdateService() {
        Service newService = new Service();
        newService.setServiceId(this.serviceEntity.getService().getServiceId());
        ResourceMetadata newMetadata = new ResourceMetadata();
        newService.setResourceMetadata(newMetadata);

        String resultServiceId = this.accessor.updateService(newService);

    }

    @Test
    public void testListServices() {
        Mockito.when(this.serviceDao.getAllAvailableServices()).thenReturn(Collections.singletonList(this.serviceEntity));

        List<Service> services = this.accessor.list();
    }

    @Test
    public void testGetServices() {
        Page<ServiceEntity> userAndKeywordPage = Mockito.mock(Page.class);

        Mockito.when(userAndKeywordPage.iterator()).thenReturn(Collections.singletonList(this.serviceEntity).iterator());

        Mockito.when(this.serviceDao.getServiceListForUserAndKeyword(
                Mockito.eq("my_keyword"),
                Mockito.eq("my_username"),
                Mockito.any(Pagination.class)
        )).thenReturn(userAndKeywordPage);
        Mockito.when(this.serviceDao.getServiceListByUser(
                Mockito.eq("my_username"),
                Mockito.any(Pagination.class)))
                .thenReturn(userAndKeywordPage);
        Mockito.when(this.serviceDao.getServiceListByKeyword(
                Mockito.eq("my_keyword"),
                Mockito.any(Pagination.class)))
                .thenReturn(userAndKeywordPage);
        Mockito.when(this.serviceDao.getServiceList(Mockito.any(Pagination.class)))
                .thenReturn(userAndKeywordPage);

        PiazzaResponse responseUserAndKeyword = this.accessor.getServices(1, 25, "asc", "id", "my_keyword", "my_username");
        PiazzaResponse responseUsername = this.accessor.getServices(1, 25, "asc", "id", "", "my_username");
        PiazzaResponse responseKeyword = this.accessor.getServices(1, 25, "asc", "id", "my_keyword", "");
        PiazzaResponse responseAll = this.accessor.getServices(1, 25, "asc", "id", "", "");

    }

    @Test
    public void testGetServiceById() {
        Service resultService = this.accessor.getServiceById(this.serviceEntity.getService().getServiceId());

        try {
            this.accessor.getServiceById("an_invalid_id");
            org.assertj.core.api.Fail.fail("Expected a runtime exception.");
        } catch (ResourceAccessException ex) {
            System.out.println(ex);
        }
    }

    @Test
    public void testAddAsyncServiceInstance() {
        this.accessor.addAsyncServiceInstance(new AsyncServiceInstance());
    }

    @Test
    public void testGetInstanceByJobId() {
        this.accessor.getInstanceByJobId(this.asyncServiceInstance.getJobId());
        this.accessor.getInstanceByJobId("an_invalid_id");
    }

    @Test
    public void testUpdateAsyncServiceInstance() {
        AsyncServiceInstance newInstance = new AsyncServiceInstance();
        newInstance.setJobId(this.asyncServiceInstance.getJobId());

        this.accessor.updateAsyncServiceInstance(newInstance);
    }

    @Test
    public void testDeleteAsyncServiceInstance() {
        this.accessor.deleteAsyncServiceInstance(this.asyncServiceInstance.getJobId());
    }

    @Test
    public void testGetStaleServiceInstances() {
        Mockito.when(this.asyncServiceInstanceDao.getStaleServiceInstances(Mockito.anyLong()))
                .thenReturn(Collections.singletonList(this.asyncServiceInstanceEntity));

        List<AsyncServiceInstance> resultList = this.accessor.getStaleServiceInstances();
    }

    @Test
    public void testGetTaskManagedServies() {
        Mockito.when(this.serviceDao.getAllTaskManagedServices())
                .thenReturn(Collections.singletonList(this.serviceEntity));

        List<Service> resultServices = this.accessor.getTaskManagedServices();
    }

    @Test
    public void testGetNextJobInServiceQueue() {
        ServiceJobEntity serviceJobEntity = new ServiceJobEntity();
        serviceJobEntity.setServiceId("my_service_id");
        serviceJobEntity.setServiceJob(new ServiceJob());

        Mockito.when(this.serviceJobDao.getNextJobInServiceQueue(serviceJobEntity.getServiceId()))
                .thenReturn(serviceJobEntity);

        this.accessor.getNextJobInServiceQueue(serviceJobEntity.getServiceId());
        this.accessor.getNextJobInServiceQueue("an_invalid_service");
    }

    @Test
    public void testGetTimedOutServiceJobs() {
        ServiceJobEntity serviceJobEntity = new ServiceJobEntity();
        serviceJobEntity.setServiceId("my_service_id");
        serviceJobEntity.setServiceJob(new ServiceJob());

        Mockito.when(this.serviceJobDao.getTimedOutServiceJobs(Mockito.anyString(), Mockito.anyLong()))
                .thenReturn(Collections.singletonList(serviceJobEntity));

        this.service.setTimeout(1000L);
        List<ServiceJob> results = this.accessor.getTimedOutServiceJobs("my_service_id");

        this.service.setTimeout(null);
        List<ServiceJob> nullResults = this.accessor.getTimedOutServiceJobs("my_service_id");
    }

    @Test
    public void testGetServiceJob() {
        ServiceJobEntity serviceJobEntity = new ServiceJobEntity();
        serviceJobEntity.setServiceId("my_service_id");
        serviceJobEntity.setServiceJob(new ServiceJob());

        Mockito.when(this.serviceJobDao.getServiceJobByServiceAndJobId("my_service_id", "my_job_id"))
                .thenReturn(serviceJobEntity);

        ServiceJob serviceJob = this.accessor.getServiceJob("my_service_id", "my_job_id");
        ServiceJob nullJob = this.accessor.getServiceJob("an_invalid_id", "my_job_id");
    }

    @Test
    public void testIncrementServiceJobTimeout() {
        ServiceJobEntity serviceJobEntity = new ServiceJobEntity();
        serviceJobEntity.setServiceId("my_service_id");
        serviceJobEntity.setServiceJob(new ServiceJob());
        serviceJobEntity.getServiceJob().setJobId("my_service_job_id");

        ServiceJob otherServiceJob = new ServiceJob();
        otherServiceJob.setJobId(serviceJobEntity.getServiceJob().getJobId());
        otherServiceJob.setServiceId(serviceJobEntity.getServiceId());

        Mockito.when(this.serviceJobDao.getServiceJobByServiceAndJobId(serviceJobEntity.getServiceId(), serviceJobEntity.getServiceJob().getJobId()))
                .thenReturn(serviceJobEntity);

        this.accessor.incrementServiceJobTimeout(serviceJobEntity.getServiceId(), otherServiceJob);
    }

    @Test
    public void testAddJobToServiceQueue() {
        this.accessor.addJobToServiceQueue("my_service_id", new ServiceJob());
    }

    @Test
    public void testRemoveJobFromServiceQueue() {
        ServiceJobEntity serviceJobEntity = new ServiceJobEntity();
        serviceJobEntity.setServiceId("my_service_id");
        serviceJobEntity.setServiceJob(new ServiceJob());

        Mockito.when(this.serviceJobDao.getServiceJobByServiceAndJobId("my_service_id", "my_job_id"))
                .thenReturn(serviceJobEntity);

        this.accessor.removeJobFromServiceQueue("my_service_id", "my_job_id");
        this.accessor.removeJobFromServiceQueue("invalid_service_id", "my_job_id");
    }

    @Test
    public void testGetJobById() {
        Job job = this.accessor.getJobById(this.job.getJobId());
        Job nullJob = this.accessor.getJobById("invalid_job_id");
    }

    @Test
    public void testCanUserAccessServiceQueue() throws InvalidInputException {

        this.service.setTaskAdministrators(null);
        boolean canAccess1 = this.accessor.canUserAccessServiceQueue("my_service_id", "my_username");
        this.service.setTaskAdministrators(new ArrayList<>());
        this.service.getTaskAdministrators().add("another_user");
        boolean canAccess2 = this.accessor.canUserAccessServiceQueue("my_service_id", "my_username");
        this.service.getTaskAdministrators().add("my_username");
        boolean canAccess3 = this.accessor.canUserAccessServiceQueue("my_service_id", "my_username");

        try {
            Mockito.when(this.serviceDao.getServiceById(Mockito.anyString())).thenThrow(ResourceAccessException.class);
            this.accessor.canUserAccessServiceQueue("m_service_id", "my_username");
            Fail.fail("Expected an exception.");
        } catch (InvalidInputException ex) {

        }
    }

    @Test
    public void testGetServiceQueueCollectionMetadata() {
        Mockito.when(this.serviceJobDao.getServiceJobCountForService("my_service_id"))
                .thenReturn(4L);

        Map<String, Object> map1 = this.accessor.getServiceQueueCollectionMetadata("invalid_service");
        Map<String, Object> map2 = this.accessor.getServiceQueueCollectionMetadata("my_service_id");
    }
}
