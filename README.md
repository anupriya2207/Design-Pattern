@Mock
    private WebClient webClient;
    
    @Mock
    private WebClient.RequestBodyUriSpec requestBodyUriSpec;
    
    @Mock
    private WebClient.RequestBodySpec requestBodySpec;
    
    @Mock
    private WebClient.ResponseSpec responseSpec;
    
    @Mock
    private WebClient.ResponseSpec.BodyToMonoSpec bodyToMonoSpec;
    
    @Mock
    private AggregatorAppGetStatusRequest request;
    
    @InjectMocks
    private YourService yourService; // The class where your execute method is

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testExecute_success() {
        // Arrange
        String appClientId = "test-client-id";
        String requestId = "1234";
        UpdateAppStatusResponse mockResponse = new UpdateAppStatusResponse(); // Assuming this is the response object
        when(request.getRequestId()).thenReturn(requestId);
        when(request.getAppClientId()).thenReturn(appClientId);
        
        // Mock the WebClient behavior
        when(webClient.get()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toEntity(UpdateAppStatusResponse.class)).thenReturn(bodyToMonoSpec);
        when(bodyToMonoSpec.block()).thenReturn(ResponseEntity.ok(mockResponse));

        // Act
        UpdateAppStatusResponse response = yourService.execute(request);

        // Assert
        assertNotNull(response);
        assertEquals(mockResponse, response);
        verify(webClient, times(1)).get();
    }

    @Test
    void testExecute_WebClientResponseException() {
        // Arrange
        String appClientId = "test-client-id";
        String requestId = "1234";
        when(request.getRequestId()).thenReturn(requestId);
        when(request.getAppClientId()).thenReturn(appClientId);

        // Mock WebClient to throw WebClientResponseException
        when(webClient.get()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toEntity(UpdateAppStatusResponse.class)).thenReturn(bodyToMonoSpec);
        when(bodyToMonoSpec.block()).thenThrow(WebClientResponseException.class);

        // Act & Assert
        Exception exception = assertThrows(WebClientResponseException.class, () -> {
            yourService.execute(request);
        });

        assertNotNull(exception);
    }

    @Test
    void testExecute_genericException() {
        // Arrange
        String appClientId = "test-client-id";
        String requestId = "1234";
        when(request.getRequestId()).thenReturn(requestId);
        when(request.getAppClientId()).thenReturn(appClientId);

        // Mock WebClient to throw a generic exception
        when(webClient.get()).thenReturn(requestBodyUriSpec);
        when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        when(requestBodySpec.headers(any())).thenReturn(requestBodySpec);
        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toEntity(UpdateAppStatusResponse.class)).thenReturn(bodyToMonoSpec);
        when(bodyToMonoSpec.block()).thenThrow(new RuntimeException("Generic error"));

        // Act & Assert
        CCARApplicationException exception = assertThrows(CCARApplicationException.class, () -> {
            yourService.execute(request);
        });

        assertEquals("GET_APP_STATUS_EXCEPTION", exception.getMessage()); // You can adapt this to match your message format
    }
