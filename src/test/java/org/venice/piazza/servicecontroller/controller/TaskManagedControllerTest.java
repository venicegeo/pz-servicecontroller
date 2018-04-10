package org.venice.piazza.servicecontroller.controller;

import exception.InvalidInputException;
import model.job.type.ExecuteServiceJob;
import model.response.PiazzaResponse;
import model.service.metadata.Service;
import model.status.StatusUpdate;
import org.apache.catalina.Server;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.venice.piazza.servicecontroller.data.accessor.DatabaseAccessor;
import org.venice.piazza.servicecontroller.taskmanaged.ServiceTaskManager;
import util.PiazzaLogger;

public class TaskManagedControllerTest {

    private ExecuteServiceJob executeServiceJob;

    @Mock
    private PiazzaLogger piazzaLogger;
    @Mock
    private ServiceTaskManager serviceTaskManager;
    @Mock
    private DatabaseAccessor databaseAccessor;

    @InjectMocks
    private TaskManagedController controller;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        this.executeServiceJob = new ExecuteServiceJob();
        this.executeServiceJob.setJobId("my_job_id");
    }

    @Test
    public void testGetNextServiceJobFromQueue() throws InvalidInputException{
        Mockito.when(this.databaseAccessor.canUserAccessServiceQueue(Mockito.any(), Mockito.eq("my_username")))
                .thenReturn(true);
        Mockito.when(this.serviceTaskManager.getNextJobFromQueue("my_service_id"))
                .thenReturn(this.executeServiceJob);
        Mockito.when(this.serviceTaskManager.getNextJobFromQueue("null_service_id"))
                .thenReturn(null);
        Mockito.when(this.serviceTaskManager.getNextJobFromQueue("invalidInputException_id"))
                .thenThrow(InvalidInputException.class);
        Mockito.when(this.serviceTaskManager.getNextJobFromQueue("unknownException_id"))
                .thenThrow(Exception.class);


        ResponseEntity<PiazzaResponse> responseUnauthorized = this.controller.getNextServiceJobFromQueue("an_invalid_user", "my_service_id");

        ResponseEntity<PiazzaResponse> responseValid = this.controller.getNextServiceJobFromQueue("my_username", "my_service_id");

        ResponseEntity<PiazzaResponse> responseNull = this.controller.getNextServiceJobFromQueue("my_username", "null_service_id");

        ResponseEntity<PiazzaResponse> responseInvalidInput = this.controller.getNextServiceJobFromQueue("my_username", "invalidInputException_id");

        ResponseEntity<PiazzaResponse> responseUnknownError = this.controller.getNextServiceJobFromQueue("my_username", "unknownException_id");
    }

    @Test
    public void testUpdateServiceJobStatus() throws InvalidInputException {

        Mockito.when(this.databaseAccessor.canUserAccessServiceQueue(Mockito.any(), Mockito.eq("my_username")))
                .thenReturn(true);

        StatusUpdate statusUpdate = new StatusUpdate();
        statusUpdate.setStatus("the_job_status");

        StatusUpdate badStatusUpdate = new StatusUpdate();

        Mockito.doThrow(ResourceAccessException.class).when(this.serviceTaskManager)
                .processStatusUpdate(Mockito.eq("resourceAccessEx"), Mockito.anyString(), Mockito.any(StatusUpdate.class));
        Mockito.when(this.databaseAccessor.canUserAccessServiceQueue(Mockito.eq("invalidInputEx"), Mockito.anyString()))
                .thenThrow(InvalidInputException.class);
        Mockito.when(this.databaseAccessor.canUserAccessServiceQueue(Mockito.eq("unknownEx"), Mockito.anyString()))
                .thenThrow(Exception.class);

        ResponseEntity<PiazzaResponse> responseUnathorized = this.controller.updateServiceJobStatus("an_invalid_user", "my_service_id", "my_job_id", statusUpdate);

        ResponseEntity<PiazzaResponse> responseValid = this.controller.updateServiceJobStatus("my_username", "my_service_id", "my_job_id", statusUpdate);

        ResponseEntity<PiazzaResponse> responseBadStatus = this.controller.updateServiceJobStatus("my_username", "my_service_id", "my_job_id", badStatusUpdate);

        ResponseEntity<PiazzaResponse> responseInvalidInput = this.controller.updateServiceJobStatus("my_username", "invalidInputEx", "my_job_id", statusUpdate);

        ResponseEntity<PiazzaResponse> responseUnknownEx = this.controller.updateServiceJobStatus("my_username", "unknownEx", "my_job_id", statusUpdate);
    }

    @Test
    public void testGetServiceQueueData() throws InvalidInputException {
        Service unmanagedService = new Service();
        unmanagedService.setIsTaskManaged(false);

        Service myService = new Service();
        myService.setIsTaskManaged(true);

        Mockito.when(this.databaseAccessor.canUserAccessServiceQueue(Mockito.any(), Mockito.eq("my_username")))
                .thenReturn(true);
        Mockito.when(this.databaseAccessor.getServiceById("my_service_id")).thenReturn(myService);
        Mockito.when(this.databaseAccessor.getServiceById("unmanagedService")).thenReturn(unmanagedService);
        Mockito.when(this.databaseAccessor.getServiceById("unknownException")).thenThrow(Exception.class);

        ResponseEntity responseNoAccess = this.controller.getServiceQueueData("an_invalid_user", "my_service_id");

        ResponseEntity respUnmanaged = this.controller.getServiceQueueData("my_username", "unmanagedService");

        ResponseEntity respManaged = this.controller.getServiceQueueData("my_username", "my_service_id");

        ResponseEntity responseUnknownEx = this.controller.getServiceQueueData("my_username", "unknownException");
    }

}
