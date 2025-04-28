




@ExtendWith(MockitoExtension.class)
public class GetAppDetailsServiceTest {

    @Mock
    private WebClient webClient;

    @Mock
    private WebClient.RequestBodyUriSpec requestBodyUriSpec;

    @Mock
    private WebClient.RequestHeadersSpec requestHeadersSpec;

    @Mock
    private WebClient.ResponseSpec responseSpec;

    @InjectMocks
    private GetAppDetailsService getAppDetailsService;

    @BeforeEach
    void setUp() {
        lenient().when(webClient.mutate().codecs(any())).thenReturn((WebClient.Builder) webClient);
        lenient().when(webClient.post()).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.headers(any())).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.bodyValue(any())).thenReturn(requestHeadersSpec);
        lenient().when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
    }

    @Test
    void testGetAppDetails_Success() {
        GetAppDetailsResponse mockResponse = new GetAppDetailsResponse();
        ResponseEntity<GetAppDetailsResponse> responseEntity = new ResponseEntity<>(mockResponse, HttpStatus.OK);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenReturn(Mono.just(responseEntity));

        ResponseEntity<GetAppDetailsResponse> response = getAppDetailsService.getAppDetails();
        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
    }

    @Test
    void testGetAppDetails_ErrorWhileCallingCPAC(){
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(new RuntimeException("Internal Server Error"));

        Exception exception = assertThrows(RuntimeException.class, () -> getAppDetailsService.getAppDetails());
        assertEquals("Error while calling CPAC app details service", exception.getMessage());
    }

    @Test
    void testGetAppDetails_BadRequest() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.BAD_REQUEST.value(), "Bad Request", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);

        Exception thrownException = assertThrows(SubSystemException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Bad Request"));
    }

    @Test
    void testGetAppDetails_NotFound() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.NOT_FOUND.value(), "Not Found", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);

        Exception thrownException = assertThrows(RuntimeException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Not Found"));
    }


    @Test
    void testGetAppDetails_UnsupportedMediaType() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.UNSUPPORTED_MEDIA_TYPE.value(), "Unsupported Media Type", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);

        Exception thrownException = assertThrows(SubSystemException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Unsupported Media Type"));
    }

    @Test
    void testGetAppDetails_InternalServerError() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.INTERNAL_SERVER_ERROR.value(), "Internal Server Error", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);

        Exception thrownException = assertThrows(RuntimeException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Internal Server Error"));
    }

    @Test
    void testGetAppDetails_UnknownException() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.I_AM_A_TEAPOT.value(), "Unknown Error", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);

        Exception thrownException = assertThrows(InternalSystemException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Unknown Error"));
    }

}

webClient.mutate().codecs(clientCodecConfigurer -> clientCodecConfigurer.defaultCodecs().maxInMemorySize(MEMORY_SIZE)).build();



WebClient.Builder mockBuilder = mock(WebClient.Builder.class);
when(webClient.mutate()).thenReturn(mockBuilder);
when(mockBuilder.codecs(any())).thenReturn(mockBuilder);
when(mockBuilder.build()).thenReturn(webClient);



WebClient.Builder webClientBuilder = mock(WebClient.Builder.class);
lenient().when(webClient.mutate()).thenReturn(webClientBuilder);
lenient().when(webClientBuilder.codecs(any())).thenReturn(webClientBuilder);
lenient().when(webClientBuilder.build()).thenReturn(webClient);





lenient().when(webClient.mutate().codecs(any()).build()).thenReturn(webClient);  // now works because deep stub
        lenient().when(webClient.post()).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.headers(any())).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.bodyValue(any())).thenReturn(requestHeadersSpec);
        lenient().when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);








        public GetAppDetailsService(@Qualifier(WEB_CLIENT_BEAN) WebClient webClient,
                                @Value("${daps.webclient.services.getAllAppDetailsService.url}") String url,
                                @Value("${host.cpac}") String host,
                                ObjectMapper objectMapper,
                                @Value("${http.protocol}") String httpProtocol) {
        this.webClient = webClient.mutate().codecs(clientCodecConfigurer -> clientCodecConfigurer.defaultCodecs().maxInMemorySize(MEMORY_SIZE)).build();
        this.url = url;
        this.host = host;

        this.httpProtocol = httpProtocol;
        this.objectMapper = objectMapper;
    }

    public ResponseEntity<GetAppDetailsResponse> getAppDetails() {

        ResponseEntity<GetAppDetailsResponse> response = null;
        try {
            response = webClient
                    .post()
                    .uri(generateURI())
                    .headers(httpHeaders -> httpHeaders.addAll(getHeaders()))
                    .bodyValue(Collections.emptyList())
                    .retrieve()
                    .toEntity(GetAppDetailsResponse.class)
                    .block();





        WebClient webClient = webClientBuilder
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(MEMORY_SIZE))
            .build();
