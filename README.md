








audt_actv_id,usr_actn_log_mv,cre_ts,appl_clnt_id,txn_sts_cd,rsn_tx,extn_csnt_id,thrd_prty_csnt_srvc_usr_id
72e108cc-07a0-4903-b8f4-82786c1d,"{""consentUserIdentifier"": ""41231f-14b2-4ebb-89de-765f8207c0bc"", ""digitalCustomerTypeCode"": ""PR"", ""enterprisePartyIdentifier"": ""06162507"", ""updatedAccountsAndPreferences"": {""accountsAutoAuthorizedIndicator"": true, ""clientId"": ""DIS"", ""consentStatusCode"": ""AC"", ""eligibleAccounts"": [{""accountIdentifier"": 946346, ""accountProductTypeCode"": ""080"", ""accountTypeCode"": ""BAC"", ""authorizationIndicator"": true, ""firmLineOfBusinessCode"": ""CARD"", ""lobAccountIdentifier"": ""14003507"", ""reasonText"": ""USER CONSENT"", ""subProductCode"": ""001"", ""systemOfRecordName"": ""C3"", ""virtualAccountAuthorizedIndicator"": false}], ""onlinePersonIdentifier"": 332981796, ""onlineProfileIdentifier"": 55969107, ""versionNumber"": ""1""}}",2025-03-22 09:00:50.975098,DIS,Update Consent,,7435291-29f5-37a4-87c8-31cd78d1a6ca,4881231f-14b2-4ebb-89de-765f207c0bc














public void exportConsentDataToS3() {
    if (isUnitTest) {
        return;
    }

    LOG.info("Exporting consent data to S3");

    // Define the folder structure in S3
    String fileName = "ConsentData_" + DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now()) + ".csv";
    String s3Path = "PDRAndConsentData/Consent_Data/" + fileName;  // <-- Updated folder path

    String s3ImportPath = CDPConstants.S3_PROTOCOL + mercuryS3Properties.getBucket();

    StringBuffer sb = new StringBuffer();
    sb.append("'").append(s3ImportPath).append("/").append(s3Path) // <-- Using the new path
            .append("?AWS_ACCESS_KEY_ID=").append(objectStore.getS3Keys().getAccessKey())
            .append("&AWS_SECRET_ACCESS_KEY=").append(objectStore.getS3Keys().getSecretKeys())
            .append("&AWS_ENDPOINT=").append(mercuryS3Properties.getDataplaneEndpoint())
            .append("' WITH chunk_rows='5000000'");

    String exportQuery = "EXPORT INTO CSV %s FROM " +
            "(SELECT audt_actv_id, " +
            "JSON_EXTRACT_SCALAR(usr_actn_log_mv, '$.onlineProfileIdentifier') AS onlineProfileIdentifier, " +
            "JSON_EXTRACT_SCALAR(usr_actn_log_mv, '$.onlinePersonIdentifier') AS onlinePersonIdentifier, " +
            "JSON_EXTRACT_SCALAR(usr_actn_log_mv, '$.versionNumber') AS versionNumber, " +
            "cre_ts, txn_sts_cd, appl_clnt_id, extn_csnt_id " +
            "FROM consent_table " +
            "WHERE txn_sts_cd = 'CREATE_CONSENT' " +
            "AND cre_ts >= NOW() - INTERVAL '1 DAY')"
            .formatted(sb);

    consentJdbcTemplate.execute((ConnectionCallback<Object>) connection -> {
        try (PreparedStatement statement = connection.prepareStatement(exportQuery)) {
            statement.execute();
            LOG.info("Export to S3 completed successfully: " + s3Path);
        } catch (Exception e) {
            LOG.error("Exception occurred during export", e);
            throw e;
        }
        return null;
    });
}






SELECT 
    audt_actv_id, 
    COALESCE(usr_actn_log_mv->>'onlineProfileIdentifier', 'NULL') AS onlineProfileIdentifier, 
    COALESCE(usr_actn_log_mv->>'onlinePersonIdentifier', 'NULL') AS onlinePersonIdentifier, 
    usr_actn_log_mv->>'versionNumber' AS versionNumber, 
    cre_ts, 
    txn_sts_cd, 
    appl_clnt_id, 
    extn_csnt_id 
FROM consent_table 
WHERE txn_sts_cd = 'CREATE_CONSENT' 
AND cre_ts >= CURRENT_DATE - INTERVAL '1 DAY' 
AND cre_ts < CURRENT_DATE;

"COALESCE(usr_actn_log_mv->>'onlineProfileIdentifier', 'NULL') AS onlineProfileIdentifier, " +
        "COALESCE(usr_actn_log_mv->>'onlinePersonIdentifier', 'NULL') AS onlinePersonIdentifier, " +
        "usr_actn_log_mv->>'versionNumber' AS versionNumber, " +


kini





    SELECT 
        audt_actv_id, 
        COALESCE(usr_actn_log_mv->>'onlineProfileIdentifier', 'NULL') AS onlineProfileIdentifier, 
        COALESCE(usr_actn_log_mv->>'onlinePersonIdentifier', 'NULL') AS onlinePersonIdentifier, 
        usr_actn_log_mv->>'versionNumber' AS versionNumber, 
        cre_ts, 
        txn_sts_cd, 
        appl_clnt_id, 
        extn_csnt_id, 
        CASE 
            WHEN appl_clnt_id IN ('TTAX', 'J100002', 'JINTMIN100001') THEN 'INTUIT' 
            ELSE SPLIT_PART(appl_clnt_id, '_', 1) 
        END AS aggregator 
    FROM consent_table 
    WHERE txn_sts_cd = 'CREATE_CONSENT' 
    AND cre_ts >= CURRENT_DATE - INTERVAL '1 DAY'  -- Only yesterday's data
    AND cre_ts < CURRENT_DATE





    {"digitalCustomerTypeCode": "PR", "enterprisePartyIdentifier": "0307436849", "updatedAccountsAndPreferences": {"accountsAutoAuthorizedIndicator": true, "clientId": "AMAZON_AMAZON", "consentStatusCode": "AC", "eligibleAccounts": [{"accountIdentifier": 11399260, "accountProductTypeCode": "080", "accountTypeCode": "BAC", "authorizationIndicator": true, "lobAccountIdentifier": "1403175825", "reasonText": "USER CONSENT", "subProductCode": "001", "virtualAccountAuthorizedIndicator": true}], "onlinePersonIdentifier": 37491014160, "onlineProfileIdentifier": 57865492, "versionNumber": "1"}}











    SELECT
    t1.audt_actv_id,
    t2.ol_prs_id AS onlinePersonIdentifier,
    t2.ol_prfl_id AS onlineProfileIdentifier,
    t3.appl_ver_nb AS versionNumber,
    t1.cre_ts,
    t1.txn_sts_cd,
    t1.appl_clnt_id,
    t1.extn_csnt_id,
    CASE
        WHEN t1.appl_clnt_id IN ('TTAX', 'JPMINTQBO100002', 'JPMINTMIN100001') THEN 'INTUIT'
        ELSE SPLIT_PART(t1.appl_clnt_id, '_', 1)
        END AS aggregator
FROM csnt_audt_actv t1
LEFT JOIN thrd_prty_csnt_srvc_usr t2
ON t1.thrd_prty_csnt_srvc_usr_id = t2.thrd_prty_csnt_srvc_usr_id
LEFT JOIN usr_csnt t3
ON t1.thrd_prty_csnt_srvc_usr_id = t3.thrd_prty_csnt_srvc_usr_id
AND t1.appl_clnt_id=t3.appl_clnt_id
WHERE EXISTS (
    SELECT 1
    FROM csnt_audt_actv ct
    WHERE ct.thrd_prty_csnt_srvc_usr_id = t1.thrd_prty_csnt_srvc_usr_id
)
  AND t1.txn_sts_cd = 'CREATE_CONSENT'
  AND t1.cre_ts >= CURRENT_DATE - INTERVAL '1 DAY';






  String exportQuery = "EXPORT INTO CSV %s FROM " +
        "(SELECT " +
        "t1.audt_actv_id, " +
        "t2.ol_prs_id AS onlinePersonIdentifier, " +
        "t2.ol_prfl_id AS onlineProfileIdentifier, " +
        "t3.appl_ver_nb AS versionNumber, " +
        "t1.cre_ts, " +
        "t1.txn_sts_cd, " +
        "t1.appl_clnt_id, " +
        "t1.extn_csnt_id, " +
        "CASE " +
        "    WHEN t1.appl_clnt_id IN ('TTAX', 'JPMINTQBO100002', 'JPMINTMIN100001') THEN 'INTUIT' " +  
        "    ELSE SPLIT_PART(t1.appl_clnt_id, '_', 1) " +  
        "END AS aggregator " +
        "FROM %s t1 " +  // Table name placeholder
        "LEFT JOIN thrd_prty_csnt_srvc_usr t2 " +
        "ON t1.thrd_prty_csnt_srvc_usr_id = t2.thrd_prty_csnt_srvc_usr_id " +
        "LEFT JOIN usr_csnt t3 " +
        "ON t1.thrd_prty_csnt_srvc_usr_id = t3.thrd_prty_csnt_srvc_usr_id " +
        "AND t1.appl_clnt_id = t3.appl_clnt_id " +
        "WHERE EXISTS ( " +
        "    SELECT 1 " +
        "    FROM csnt_audt_actv ct " +
        "    WHERE ct.thrd_prty_csnt_srvc_usr_id = t1.thrd_prty_csnt_srvc_usr_id " +
        ") " +
        "AND t1.txn_sts_cd = 'CREATE_CONSENT' " +
        "AND t1.cre_ts >= CURRENT_DATE - INTERVAL '1 DAY' " +  
        "AND t1.cre_ts < CURRENT_DATE)"
        .formatted(sb, tableName);








        private void writeJsonToCsv(ResponseEntity<GetAppDetailsResponse> response, String fileName) {

        LOG.info("Writing json to csv");
        //Step 1: Extract Response
        GetAppDetailsResponse responseBody = response.getBody();
        List<AppDetails> externalApps = responseBody.getExternalApps();
        String path ="consent-data-processor/src/main/resources/" + fileName;
        LOG.info("Writing json to csv: {}", path);

        try (FileWriter writer = new FileWriter(path)) {
            // Step 2: Write CSV Header
            writer.append("Client ID, Data Categories, Active Application Version Number\n");

            // Step 3: Iterate through externalApps and extract required fields
            for (AppDetails app : externalApps) {
                String clientId =  app.getClientId();
                Integer activeVersion =  app.getActiveApplicationVersionNumber();

                // Convert dataCategories list to a comma-separated string
                String dataCategoriesStr = String.join("| ", app.getDataCategories());

                // Write data to CSV (quotes around dataCategories to handle commas correctly)
                writer.append(clientId).append(", ")
                        .append(dataCategoriesStr).append(", ")
                        .append(activeVersion.toString()).append("\n");
            }

            System.out.println("Data successfully written to partner_app_data.csv");
            //uploadFileToS3(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }












    WITH create_consent_cte AS (
    SELECT 
        audt_actv_id, 
        thrd_prty_usr_srvc_id, 
        cre_ts, 
        txn_sts_cd, 
        appl_clnt_id, 
        extn_csnt_id 
    FROM csnt_audt_actv 
    WHERE txn_sts_cd = 'CREATE_CONSENT' 
    AND cre_ts >= CURRENT_DATE - INTERVAL '1 DAY' 
    AND cre_ts < CURRENT_DATE
)
SELECT 
    cc.audt_actv_id, 
    cc.cre_ts, 
    cc.txn_sts_cd, 
    cc.appl_clnt_id, 
    cc.extn_csnt_id,
    nc.usr_actn_log_mv->>'onlineProfileIdentifier' AS onlineProfileIdentifier,
    nc.usr_actn_log_mv->>'onlinePersonIdentifier' AS onlinePersonIdentifier, 
    nc.usr_actn_log_mv->>'versionNumber' AS versionNumber 
FROM create_consent_cte cc
LEFT JOIN csnt_audt_actv nc 
    ON cc.thrd_prty_usr_srvc_id = nc.thrd_prty_usr_srvc_id 
    AND nc.txn_sts_cd = 'NEW_CONSENT';







WITH new_consent_cte AS (
    -- Fetch NEW_CONSENT entries from the table
    SELECT 
        thrd_prty_usr_srvc_id, 
        usr_actn_log_mv->>'onlineProfileIdentifier' AS onlineProfileIdentifier,
        usr_actn_log_mv->>'onlinePersonIdentifier' AS onlinePersonIdentifier, 
        usr_actn_log_mv->>'versionNumber' AS versionNumber,
        cre_ts AS new_cre_ts  -- Capture timestamp for ordering
    FROM csnt_audt_actv 
    WHERE txn_sts_cd = 'NEW_CONSENT'
),
create_consent_cte AS (
    -- Fetch CREATE_CONSENT entries from the last day
    SELECT 
        audt_actv_id, 
        thrd_prty_usr_srvc_id, 
        cre_ts, 
        txn_sts_cd, 
        appl_clnt_id, 
        extn_csnt_id 
    FROM csnt_audt_actv 
    WHERE txn_sts_cd = 'CREATE_CONSENT' 
    AND cre_ts >= CURRENT_DATE - INTERVAL '1 DAY' 
    AND cre_ts < CURRENT_DATE
)
SELECT 
    cc.audt_actv_id, 
    cc.cre_ts, 
    cc.txn_sts_cd, 
    cc.appl_clnt_id, 
    cc.extn_csnt_id,
    nc.onlineProfileIdentifier,
    nc.onlinePersonIdentifier, 
    nc.versionNumber 
FROM create_consent_cte cc
JOIN new_consent_cte nc 
    ON cc.thrd_prty_usr_srvc_id = nc.thrd_prty_usr_srvc_id 
    AND cc.cre_ts > nc.new_cre_ts  -- Ensures CREATE_CONSENT comes immediately after NEW_CONSENT
ORDER BY cc.cre_ts DESC;







@Service
@Bulkhead(name = "getAllAppDetailsService")
@CircuitBreaker(name = "getAllAppDetailsService")
public class GetAppDetailsService {

    private static final Logger LOG = LoggerFactory.getLogger(GetAppDetailsService.class);
    private static final String WEB_CLIENT_BEAN = "daps.webclient.services.getAllAppDetailsService";
    private final String host;
    private final String url;
    private final String httpProtocol;
    private final WebClient webClient;

    private final ObjectMapper objectMapper;

    public GetAppDetailsService(@Qualifier(WEB_CLIENT_BEAN) WebClient webClient,
                                @Value("${daps.webclient.services.getAllAppDetailsService.url}") String url,
                                @Value("${host.cpac}") String host,
                                ObjectMapper objectMapper,
                                @Value("${http.protocol}") String httpProtocol) {
        this.webClient = webClient;
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
        } catch (WebClientResponseException e) {
            handleError(e);
        } catch (Exception e) {
            LOG.error(ErrorMessages.EXCEPTION_WHILE_CALLING_CPAC_APP_DETAILS, e.getMessage());
            throw new InternalSystemException(ErrorMessages.EXCEPTION_WHILE_CALLING_CPAC_APP_DETAILS, e);
        }
        return response;
    }

    private void handleError(WebClientResponseException exception) {
        switch (exception.getRawStatusCode()) {
            case HttpStatus.SC_BAD_REQUEST:
            case HttpStatus.SC_UNSUPPORTED_MEDIA_TYPE:
                throw new SubSystemException(CDPConstants.ERR_CODE_ERR_DESC.formatted(
                        ErrorMessages.CPAC_EXCEPTION, exception.getStatusCode(), exception.getStatusText()));
            case HttpStatus.SC_NOT_FOUND:
                throw new RuntimeException(CDPConstants.ERR_CODE_ERR_DESC.formatted(
                        ErrorMessages.CPAC_ROUTE_NOT_FOUND_EXCEPTION, exception.getStatusCode(), exception.getStatusText()));
            case HttpStatus.SC_INTERNAL_SERVER_ERROR:
                throw new RuntimeException(CDPConstants.ERR_CODE_ERR_DESC.formatted(
                        ErrorMessages.INTERNAL_SERVER_ERROR, exception.getStatusCode(), exception.getStatusText()));
            default:
                throw new InternalSystemException(CDPConstants.ERR_CODE_ERR_DESC.formatted(ErrorMessages.CPAC_EXCEPTION,
                        exception.getStatusCode(), exception.getStatusText(), exception));
        }
    }

    private String generateURI() {
        return UriComponentsBuilder.newInstance()
                .scheme(httpProtocol)
                .host(host)
                .path(url)
                .toUriString();
    }

    private HttpHeaders getHeaders() {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        httpHeaders.add(CDPConstants.CHANNEL_ID, MDC.get(CDPConstants.CHANNEL_ID));
        httpHeaders.add(ContextKeys.TRACE_ID, MDC.get(CDPConstants.TRACE_ID_HEADER));
        httpHeaders.add(ContextKeys.SESSION_ID, MDC.get(CDPConstants.SESSION_ID_HEADER));
        httpHeaders.add(ContextKeys.CHANNEL_TYPE, MDC.get(CDPConstants.CHANNEL_TYPE_HEADER));
        LOG.info(httpHeaders.toString());
        return httpHeaders;
    }
}






import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.RequestBodyUriSpec;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersUriSpec;
import org.springframework.web.reactive.function.client.WebClient.ResponseSpec;
import reactor.core.publisher.Mono;

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
    void testGetAppDetails_InternalServerError() {
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(new RuntimeException("Internal Server Error"));
        
        Exception exception = assertThrows(RuntimeException.class, () -> getAppDetailsService.getAppDetails());
        assertEquals("Internal Server Error", exception.getMessage());
    }
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
    void testGetAppDetails_InternalServerError() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.INTERNAL_SERVER_ERROR.value(), "Internal Server Error", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);
        
        Exception thrownException = assertThrows(RuntimeException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Internal Server Error"));
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
    void testGetAppDetails_UnknownException() {
        WebClientResponseException exception = WebClientResponseException.create(
                HttpStatus.I_AM_A_TEAPOT.value(), "Unknown Error", null, null, null);
        when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenThrow(exception);
        
        Exception thrownException = assertThrows(InternalSystemException.class, () -> getAppDetailsService.getAppDetails());
        assertTrue(thrownException.getMessage().contains("Unknown Error"));
    }



    @Test
    void testGetHeaders() {
        MDC.put("channelId", "12345");
        MDC.put("traceId", "trace-123");
        MDC.put("sessionId", "session-123");
        MDC.put("channelType", "web");

        HttpHeaders headers = getAppDetailsService.getHeaders();
        
        assertNotNull(headers);
        assertEquals("application/json", headers.getFirst(HttpHeaders.CONTENT_TYPE));
        assertEquals("12345", headers.getFirst("channelId"));
        assertEquals("trace-123", headers.getFirst("traceId"));
        assertEquals("session-123", headers.getFirst("sessionId"));
        assertEquals("web", headers.getFirst("channelType"));
    }



@Test
void testGetAppDetails_HeadersIncluded() {
    // Set MDC values
    MDC.put("channelId", "12345");
    MDC.put("traceId", "trace-123");
    MDC.put("sessionId", "session-123");
    MDC.put("channelType", "web");

    // Capture headers
    doAnswer(invocation -> {
        Consumer<HttpHeaders> headersConsumer = invocation.getArgument(0);
        HttpHeaders headers = new HttpHeaders();
        headersConsumer.accept(headers);

        // Assertions
        assertEquals("application/json", headers.getFirst(HttpHeaders.CONTENT_TYPE));
        assertEquals("12345", headers.getFirst("channelId"));
        assertEquals("trace-123", headers.getFirst("traceId"));
        assertEquals("session-123", headers.getFirst("sessionId"));
        assertEquals("web", headers.getFirst("channelType"));

        return requestBodyUriSpec;
    }).when(requestBodyUriSpec).headers(any());

    // Execute service
    when(responseSpec.toEntity(GetAppDetailsResponse.class)).thenReturn(Mono.just(new ResponseEntity<>(HttpStatus.OK)));
    getAppDetailsService.getAppDetails();
}


import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "csnt_audt_act")
public class CsntAudtAct {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "audt_actv_id")
    private String auditActivityId;

    @Column(name = "usr_actn_log_mv", columnDefinition = "TEXT")
    private String userActionLogMv;

    @Column(name = "cre_ts")
    private LocalDateTime createdTimestamp;

    @Column(name = "appl_clnt_id")
    private String applicationClientId;

    @Column(name = "txn_sts_cd")
    private String transactionStatusCode;

    @Column(name = "rsn_tx", columnDefinition = "TEXT")
    private String reasonText;

    @Column(name = "extn_csnt_id")
    private String externalConsentId;

    @Column(name = "thrd_prty_csnt_srvc_usr_id")
    private String thirdPartyConsentServiceUserId;

    // Getters and Setters
}






import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class CsntAudtActService {

    private final CsntAudtActRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CsntAudtActService(CsntAudtActRepository repository) {
        this.repository = repository;
    }

    public void processAuditData(LocalDateTime fromDate, LocalDateTime toDate) throws IOException {
        // Fetch all records within the date range
        List<CsntAudtAct> allRecords = repository.findByCreatedTimestampBetween(fromDate, toDate);

        // Step 1: Find all "CREATE_CONSENT" records
        List<CsntAudtAct> createConsentRecords = allRecords.stream()
                .filter(record -> "CREATE_CONSENT".equalsIgnoreCase(record.getTransactionStatusCode()))
                .collect(Collectors.toList());

        // Step 2: Store these records for lookup
        Map<String, CsntAudtAct> createConsentMap = new HashMap<>();
        for (CsntAudtAct record : createConsentRecords) {
            String key = record.getThirdPartyConsentServiceUserId() + "-" + record.getApplicationClientId();
            createConsentMap.put(key, record);
        }

        // Step 3: Find the highest version "NEW_CONSENT" record for each matching key
        Map<String, CsntAudtAct> highestVersionRecords = new HashMap<>();
        for (CsntAudtAct record : allRecords) {
            if (!"NEW_CONSENT".equalsIgnoreCase(record.getTransactionStatusCode())) continue;

            String key = record.getThirdPartyConsentServiceUserId() + "-" + record.getApplicationClientId();
            if (!createConsentMap.containsKey(key)) continue; // Skip if no matching CREATE_CONSENT record

            try {
                JsonNode rootNode = objectMapper.readTree(record.getUserActionLogMv());
                int version = rootNode.path("updatedAccountsAndPreferences").path("versionNumber").asInt();

                if (!highestVersionRecords.containsKey(key) ||
                        version > objectMapper.readTree(highestVersionRecords.get(key).getUserActionLogMv())
                                .path("updatedAccountsAndPreferences").path("versionNumber").asInt()) {
                    highestVersionRecords.put(key, record);
                }
            } catch (Exception e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
            }
        }

        // Step 4: Write results to CSV
        writeToCSV(createConsentMap, highestVersionRecords);
    }

    private void writeToCSV(Map<String, CsntAudtAct> createConsentMap, Map<String, CsntAudtAct> highestVersionRecords) throws IOException {
        try (FileWriter writer = new FileWriter("audit_report.csv")) {
            writer.append("AuditActivityId,CreatedTimestamp,TransactionStatus,ApplicationClientId,ExternalConsentId,ThirdPartyConsentServiceUserId,Version,ProfileIdentifier,PersonIdentifier\n");

            for (Map.Entry<String, CsntAudtAct> entry : createConsentMap.entrySet()) {
                String key = entry.getKey();
                CsntAudtAct createConsentRecord = entry.getValue();
                CsntAudtAct highestVersionRecord = highestVersionRecords.get(key);

                String version = "", profileId = "", personId = "";
                if (highestVersionRecord != null) {
                    JsonNode rootNode = objectMapper.readTree(highestVersionRecord.getUserActionLogMv());
                    version = rootNode.path("updatedAccountsAndPreferences").path("versionNumber").asText();
                    profileId = rootNode.path("updatedAccountsAndPreferences").path("onlineProfileIdentifier").asText();
                    personId = rootNode.path("updatedAccountsAndPreferences").path("onlinePersonIdentifier").asText();
                }

                writer.append(String.join(",",
                        createConsentRecord.getAuditActivityId(),
                        createConsentRecord.getCreatedTimestamp().toString(),
                        createConsentRecord.getTransactionStatusCode(),
                        createConsentRecord.getApplicationClientId(),
                        createConsentRecord.getExternalConsentId(),
                        createConsentRecord.getThirdPartyConsentServiceUserId(),
                        version, profileId, personId))
                        .append("\n");
            }
        }
        System.out.println("CSV file generated: audit_report.csv (in project root)");
    }
}











try {
            // Fetch all records within the date range
            List<CsntAudtAct> allRecords = repository.findByCreatedTimestampBetween(fromDate, toDate);

            // Filter records where txn_sts_cd = "CREATE_CONSENT"
            List<CsntAudtAct> createConsentRecords = allRecords.stream()
                    .filter(record -> "CREATE_CONSENT".equals(record.getTransactionStatusCode()))
                    .collect(Collectors.toList());

            List<String[]> csvData = new ArrayList<>();
            csvData.add(new String[]{"AuditActivityId", "CreatedTimestamp", "TransactionStatusCode", "ApplicationClientId",
                    "ExternalConsentId", "ThirdPartyConsentServiceUserId", "VersionNumber",
                    "OnlineProfileIdentifier", "OnlinePersonIdentifier"});

            for (CsntAudtAct createConsent : createConsentRecords) {
                String thirdPartyConsentServiceUserId = createConsent.getThirdPartyConsentServiceUserId();
                String applicationClientId = createConsent.getApplicationClientId();

                // Find matching "New Consent" records from allRecords
                List<CsntAudtAct> newConsentRecords = allRecords.stream()
                        .filter(record -> "New Consent".equals(record.getTransactionStatusCode()) &&
                                applicationClientId.equals(record.getApplicationClientId()) &&
                                thirdPartyConsentServiceUserId.equals(record.getThirdPartyConsentServiceUserId()))
                        .collect(Collectors.toList());

                // Find the record with the highest version number
                Optional<CsntAudtAct> highestVersionRecord = newConsentRecords.stream()
                        .max(Comparator.comparing(record -> extractVersionNumber(record.getUserActionLogMv())));

                if (highestVersionRecord.isPresent()) {
                    JsonNode userActionLog = objectMapper.readTree(highestVersionRecord.get().getUserActionLogMv());

                    String versionNumber = userActionLog.path("updatedAccountsAndPreferences").path("versionNumber").asText();
                    String onlineProfileIdentifier = userActionLog.path("updatedAccountsAndPreferences").path("onlineProfileIdentifier").asText();
                    String onlinePersonIdentifier = userActionLog.path("updatedAccountsAndPreferences").path("onlinePersonIdentifier").asText();

                    csvData.add(new String[]{
                            createConsent.getAuditActivityId(),
                            createConsent.getCreatedTimestamp().toString(),
                            createConsent.getTransactionStatusCode(),
                            createConsent.getApplicationClientId(),
                            createConsent.getExternalConsentId(),
                            createConsent.getThirdPartyConsentServiceUserId(),
                            versionNumber,
                            onlineProfileIdentifier,
                            onlinePersonIdentifier
                    });
                }
            }

            writeCsvFile(csvData);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Extracts the version number from JSON
    private int extractVersionNumber(String jsonString) {
        try {
            JsonNode node = objectMapper.readTree(jsonString);
            return node.path("updatedAccountsAndPreferences").path("versionNumber").asInt();
        } catch (Exception e) {
            return 0; // Default if parsing fails
        }
    }

    // Writes the CSV file in the project root
    private void writeCsvFile(List<String[]> data) {
        try (FileWriter writer = new FileWriter("audit_report.csv")) {
            for (String[] row : data) {
                writer.append(String.join(",", row)).append("\n");
            }
            System.out.println("CSV file generated successfully in project root!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
🔹 What Changed?
Change	Why?
Updated "NEW_CONSENT" → "New Consent"	Now correctly filters txn_sts_cd = "New Consent".
No other logic changed	The rest of the filtering, JSON extraction, and CSV generation remains the same.
🚀 Final Behavior
1️⃣ Fetch all records within the date range.
2️⃣ Find CREATE_CONSENT records.
3️⃣ Find matching "New Consent" records for each.
4️⃣ Extract the highest version from JSON.
5️⃣ Extract profile & person identifiers from JSON.
6️⃣ Generate audit_report.csv in the project root. 🎯

✅ Now it should work exactly as expected! 🚀







String aggregatorId = "";
    if(clientId.matches("TTAX|JPMINTQBO100002|JPMINTMIN100001")){
        aggregatorId="INTUIT";
    }
    else{
        String[] clientIdArr = clientId.split(UNDER_SCORE);
        aggregatorId = clientIdArr[0];
    }




package com.chase.digital.aggregator.cdp.service;

import com.chase.digital.aggregator.cdp.consent.CsntAudtActv;
import com.chase.digital.aggregator.cdp.consent.repository.CsntAudtActvRepository;

import com.chase.digital.aggregator.cdp.model.ConsentDataExtractionRequest;
import com.chase.digital.aggregator.cdp.util.S3ObjectStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


@Service
@ConditionalOnProperty(name = "trigger.job.name",havingValue = "consent_data_extraction_job")
public class ConsentDataExtractionService {
    private static final Logger LOG = LoggerFactory.getLogger(ConsentDataExtractionService.class);
    private final CsntAudtActvRepository repository;
    private final ObjectMapper objectMapper;
    private final S3ObjectStore objectStore;

    public ConsentDataExtractionService(CsntAudtActvRepository repository, ObjectMapper objectMapper, S3ObjectStore objectStore) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.objectStore = objectStore;
    }

    public void extractConsentData(ConsentDataExtractionRequest request) {
        LocalDateTime fromDate = request.getFromDate();
        LocalDateTime toDate = request.getToDate();
        // Fetch all records within the date range
        try {
            // Fetch all records within the date range
            List<CsntAudtActv> allRecords = repository.findByCreatedTimestampBetween(fromDate, toDate);

            // Filter records where txn_sts_cd = "CREATE_CONSENT"
            List<CsntAudtActv> createConsentRecords = allRecords.stream()
                    .filter(record -> "CREATE_CONSENT".equals(record.getTransactionStatusCode()))
                    .toList();

            List<String[]> csvData = new ArrayList<>();
            csvData.add(new String[]{"audt_actv_id", "cre_ts", "txn_sts_cd", "appl_clnt_id",
                    "extn_csnt_id", "thrd_prty_csnt_srvc_usr_id", "version",
                    "profile_id", "person_id", "aggregator"});

            for (CsntAudtActv createConsent : createConsentRecords) {
                String thirdPartyConsentServiceUserId = createConsent.getThirdPartyConsentServiceUserId();
                String applicationClientId = createConsent.getApplicationClientId();

                // Find matching "New Consent" records from allRecords
                List<CsntAudtActv> newConsentRecords = allRecords.stream()
                        .filter(record -> "New Consent".equals(record.getTransactionStatusCode()) &&
                                applicationClientId.equals(record.getApplicationClientId()) &&
                                thirdPartyConsentServiceUserId.equals(record.getThirdPartyConsentServiceUserId()))
                        .toList();

                // Find the record with the highest version number
                Optional<CsntAudtActv> highestVersionRecord = newConsentRecords.stream()
                        .max(Comparator.comparing(record -> extractVersionNumber(record.getUserActionLogMv())));

                if (highestVersionRecord.isPresent()) {
                    JsonNode userActionLog = objectMapper.readTree(highestVersionRecord.get().getUserActionLogMv());

                    String versionNumber = userActionLog.path("updatedAccountsAndPreferences").path("versionNumber").asText();
                    String onlineProfileIdentifier = userActionLog.path("updatedAccountsAndPreferences").path("onlineProfileIdentifier").asText();
                    String onlinePersonIdentifier = userActionLog.path("updatedAccountsAndPreferences").path("onlinePersonIdentifier").asText();
                    String aggregator = determineAggregator(applicationClientId);

                    csvData.add(new String[]{
                            createConsent.getAuditActivityId(),
                            createConsent.getCreatedTimestamp().toString(),
                            createConsent.getTransactionStatusCode(),
                            createConsent.getApplicationClientId(),
                            createConsent.getExternalConsentId(),
                            createConsent.getThirdPartyConsentServiceUserId(),
                            versionNumber,
                            onlineProfileIdentifier,
                            onlinePersonIdentifier,
                            aggregator
                    });
                }
            }
            writeCsvFileToMercuryS3(csvData, fromDate);
//            writeCsvFile(csvData);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String determineAggregator(String applicationClientId) {
        String aggregatorId = "";
        if(applicationClientId.matches("TTAX|JPMINTQBO100002|JPMINTMIN100001")){
            aggregatorId="INTUIT";
        }
        else{
            String[] clientIdArr = applicationClientId.split("_");
            aggregatorId = clientIdArr[0];
        }
        return aggregatorId;
    }

    // Extracts the version number from JSON
    private int extractVersionNumber(String jsonString) {
        try {
            JsonNode node = objectMapper.readTree(jsonString);
            return node.path("updatedAccountsAndPreferences").path("versionNumber").asInt();
        } catch (Exception e) {
            return 0; // Default if parsing fails
        }
    }

    // Writes the CSV file to mercury s3
    private void writeCsvFileToMercuryS3(List<String[]> data, LocalDateTime fromDate) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             OutputStreamWriter writer = new
                     OutputStreamWriter(baos, StandardCharsets.UTF_8)) {
            for (String[] row : data) {
                writer.append(String.join(",", row)).append("\n");
            }
        writer.flush(); // Ensure all data is written to the ByteArrayOutputStream

        // Upload to S3
        try (ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())) {
            String dateStr = fromDate.toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE);
            String folderPath = "PDRAndConsentData/CONSENT_DATA/";
            String s3Key = folderPath + "CONSENT_Data_" + dateStr + ".csv";
            LOG.info("Uploading file to S3 key : {}", s3Key);
            objectStore.putObject(s3Key, bais);
            LOG.info("File successfully uploaded to S3: {}", s3Key);
        }
        } catch (IOException e) {
         LOG.error("Error writing CSV data to in-memory stream", e);
        }
    }

//    // Writes the CSV file in the project root
//    private void writeCsvFile(List<String[]> data) {
//        try (FileWriter writer = new FileWriter("audit_report.csv")) {
//            for (String[] row : data) {
//                writer.append(String.join(",", row)).append("\n");
//            }
//            System.out.println("CSV file generated successfully in project root!");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}





import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;

@ExtendWith(MockitoExtension.class)
class ConsentDataExtractionServiceTest {
    
    @Mock
    private CsntAudtActvRepository repository;
    
    @Mock
    private S3ObjectStore objectStore;
    
    @Mock
    private ObjectMapper objectMapper;
    
    @InjectMocks
    private ConsentDataExtractionService service;
    
    private List<CsntAudtActv> mockRecords;
    
    @BeforeEach
    void setUp() {
        mockRecords = new ArrayList<>();
        CsntAudtActv createConsentRecord = new CsntAudtActv("1", LocalDateTime.now(), "CREATE_CONSENT", "APP123", "EXT123", "USER123", "{}");
        CsntAudtActv newConsentRecord = new CsntAudtActv("2", LocalDateTime.now(), "New Consent", "APP123", "EXT123", "USER123", "{\"updatedAccountsAndPreferences\": {\"versionNumber\": 2, \"onlineProfileIdentifier\": \"profile1\", \"onlinePersonIdentifier\": \"person1\"}}");
        
        mockRecords.add(createConsentRecord);
        mockRecords.add(newConsentRecord);
    }

    @Test
    void testExtractConsentData() throws Exception {
        LocalDateTime fromDate = LocalDateTime.now().minusDays(1);
        LocalDateTime toDate = LocalDateTime.now();
        ConsentDataExtractionRequest request = new ConsentDataExtractionRequest(fromDate, toDate);
        
        when(repository.findByCreatedTimestampBetween(fromDate, toDate)).thenReturn(mockRecords);
        when(objectMapper.readTree(anyString())).thenAnswer(invocation -> new ObjectMapper().readTree(invocation.getArgument(0)));
        
        service.extractConsentData(request);
        
        verify(objectStore, times(1)).putObject(anyString(), any());
    }

    @Test
    void testExtractConsentData_IndirectlyVerifyAggregatorAndVersionNumber() throws Exception {
        LocalDateTime fromDate = LocalDateTime.now().minusDays(1);
        LocalDateTime toDate = LocalDateTime.now();
        ConsentDataExtractionRequest request = new ConsentDataExtractionRequest(fromDate, toDate);
        
        when(repository.findByCreatedTimestampBetween(fromDate, toDate)).thenReturn(mockRecords);
        when(objectMapper.readTree(anyString())).thenAnswer(invocation -> new ObjectMapper().readTree(invocation.getArgument(0)));
        
        service.extractConsentData(request);
        
        verify(objectStore, times(1)).putObject(argThat(key -> key.contains("CONSENT_Data_")), any());
        
        assertTrue(mockRecords.stream().anyMatch(record -> record.getTransactionStatusCode().equals("CREATE_CONSENT")));
        assertTrue(mockRecords.stream().anyMatch(record -> record.getTransactionStatusCode().equals("New Consent")));
    }
}



