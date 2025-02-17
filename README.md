import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class AggregatorAppServiceTest {

    @Mock
    private WebClient webClient;

    @Mock
    private WebClient.RequestBodyUriSpec requestBodyUriSpec;

    @Mock
    private WebClient.RequestBodySpec requestBodySpec;

    @Mock
    private WebClient.ResponseSpec responseSpec;

    @InjectMocks
    private AggregatorAppService aggregatorAppService; // Assuming your class is named AggregatorAppService

    @Test
    public void testExecute_Success() {
        AggregatorAppGetStatusRequest request = new AggregatorAppGetStatusRequest();
        request.setRequestId("12345");
        request.setAppClientId("testClient");

        // Mock the WebClient behavior
        UpdateAppStatusResponse mockResponse = new UpdateAppStatusResponse(); // Mock a response
        when(webClient.get()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toEntity(UpdateAppStatusResponse.class)).thenReturn(ResponseEntity.ok(mockResponse));

        // Call the method under test
        UpdateAppStatusResponse response = aggregatorAppService.execute(request);

        // Assertions
        assertNotNull(response);
        // Additional assertions can be made depending on the fields in UpdateAppStatusResponse
    }

    @Test
    public void testExecute_WebClientResponseException() {
        AggregatorAppGetStatusRequest request = new AggregatorAppGetStatusRequest();
        request.setRequestId("12345");
        request.setAppClientId("testClient");

        // Simulate a WebClientResponseException
        when(webClient.get()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenThrow(WebClientResponseException.class);

        // Call the method under test and check if it handles the exception
        UpdateAppStatusResponse response = aggregatorAppService.execute(request);

        // In case of an exception, the response should be null
        assertNull(response);
    }

    @Test
    public void testExecute_GenericException() {
        AggregatorAppGetStatusRequest request = new AggregatorAppGetStatusRequest();
        request.setRequestId("12345");
        request.setAppClientId("testClient");

        // Simulate a generic exception
        when(webClient.get()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenThrow(RuntimeException.class);

        // Call the method under test and assert that an exception is thrown
        assertThrows(CCARApplicationException.class, () -> aggregatorAppService.execute(request));
    }
}
