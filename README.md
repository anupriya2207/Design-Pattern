{
    "externalApps": [
        {
            "clientId": "anu",
            "requestedApplicationVersionNumber": 1,
            "activeApplicationVersionNumber": 1,
            "applicationIdentifier": "aa",
            "partnerIdentifier": "aa",
            "coBrandIndicator": false,
            "displayName": "aa",
            "appAutoAuthorizeEligibilityIndicator": true,
            "preAutoAuthorizeIndicator": false,
            "experienceServiceIndicator": false,
            "dataCategories": [
                "ab",
                "bc",
                "cd",
                "de"
            ],
            "externalProductApplicationStatusCode": "ACTIVE",
            "externalConnectionIdentifier": ""
        },
        {
            "clientId": "aas",
            "requestedApplicationVersionNumber": 3,
            "activeApplicationVersionNumber": 3,
            "applicationIdentifier": "cdef",
            "partnerIdentifier": "dfgh",
            "coBrandIndicator": false,
            "displayName": "hgh",
            "appAutoAuthorizeEligibilityIndicator": true,
            "preAutoAuthorizeIndicator": false,
            "experienceServiceIndicator": false,
            "dataCategories": [
                "ab",
                "fg"
            ],
            "externalProductApplicationStatusCode": "ACTIVE",
            "externalConnectionIdentifier": ""
        },


        ]}



public static void writeJsonToCsv(String jsonResponse, String fileName) {
        ObjectMapper objectMapper = new ObjectMapper();

        try (FileWriter writer = new FileWriter(fileName)) {
            // Writing CSV header
            writer.append("Client ID, Data Categories, Active Application Version Number\n");

            // Parse JSON
            JsonNode rootNode = objectMapper.readTree(jsonResponse);
            JsonNode externalApps = rootNode.path("externalApps");

            // Iterate over each externalApp
            for (JsonNode app : externalApps) {
                String clientId = app.path("clientId").asText();
                String activeVersion = app.path("activeApplicationVersionNumber").asText();

                // Convert dataCategories array to a comma-separated string
                JsonNode dataCategoriesNode = app.path("dataCategories");
                Iterator<JsonNode> iterator = dataCategoriesNode.elements();
                StringBuilder dataCategories = new StringBuilder();

                while (iterator.hasNext()) {
                    if (dataCategories.length() > 0) {
                        dataCategories.append(", ");  // Use commas instead of semicolons
                    }
                    dataCategories.append(iterator.next().asText());
                }

                // Write to CSV (Quotes are added to properly handle commas in Data Categories)
                writer.append(clientId).append(", ")
                      .append("\"").append(dataCategories.toString()).append("\"").append(", ")
                      .append(activeVersion).append("\n");
            }

            System.out.println("Data successfully written to " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }








    Map<String, Object> responseBody = responseEntity.getBody();

        if (responseBody == null || !responseBody.containsKey("externalApps")) {
            System.out.println("Response body is empty or does not contain 'externalApps'.");
            return;
        }

        List<Map<String, Object>> externalApps = (List<Map<String, Object>>) responseBody.get("externalApps");





















        GetAppDetailsResponse responseBody = responseEntity.getBody();

        if (responseBody == null || responseBody.getExternalApps() == null) {
            System.out.println("Response body is empty or does not contain 'externalApps'.");
            return;
        }

        List<ExternalApp> externalApps = responseBody.getExternalApps();

        try (FileWriter writer = new FileWriter("output.csv")) {
            // Step 2: Write CSV Header
            writer.append("Client ID, Data Categories, Active Application Version Number\n");

            // Step 3: Iterate through externalApps and extract required fields
            for (ExternalApp app : externalApps) {
                String clientId = app.getClientId();
                int activeVersion = app.getActiveApplicationVersionNumber();

                // Convert dataCategories list to a comma-separated string
                List<String> dataCategories = app.getDataCategories();
                String dataCategoriesStr = String.join(", ", dataCategories);

                // Write data to CSV (quotes around dataCategories to handle commas correctly)
                writer.append(clientId).append(", ")
                      .append("\"").append(dataCategoriesStr).append("\"").append(", ")
                      .append(String.valueOf(activeVersion)).append("\n");
            }

            System.out.println("Data successfully written to output.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }







 List<Map<String, Object>> externalApps = (List<Map<String, Object>>) responseBody.get("externalApps");

        try (FileWriter writer = new FileWriter("output.csv")) {
            // Step 2: Write CSV Header
            writer.append("Client ID, Data Categories, Active Application Version Number\n");

            // Step 3: Iterate through externalApps and extract required fields
            for (Map<String, Object> app : externalApps) {
                String clientId = (String) app.get("clientId");
                Integer activeVersion = (Integer) app.get("activeApplicationVersionNumber");

                // Convert dataCategories list to a comma-separated string
                List<String> dataCategories = (List<String>) app.get("dataCategories");
                String dataCategoriesStr = String.join(", ", dataCategories);

                // Write data to CSV (quotes around dataCategories to handle commas correctly)
                writer.append(clientId).append(", ")
                      .append("\"").append(dataCategoriesStr).append("\"").append(", ")
                      .append(activeVersion.toString()).append("\n");
            }

            System.out.println("Data successfully written to output.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }










    public void importDataToS3() throws FileNotFoundException{
        if(isUnitTest){
            return;
        }
        truncateTanTable(dbName+"."+dbSchema +CDPConstants.PROFILE_TABLE);
        LOG.info("Exporting data to S3");
        String personProfileMapping = CDPConstants.prefix + FileNames.PERSON_PROFILE + DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now());
        String tableName = consentdbName+"."+consentdbSchema+CDPConstants.CONSENT_PROFILE_TABLE;
        //Set<String> keys = objectStore.getAllKeys(personProfileMapping);
        String s3ImportPath = CDPConstants.S3_PROTOCOL + mercuryS3Properties.getBucket();
        StringBuffer sb = new StringBuffer();
        sb.append("'").append(s3ImportPath).append("/").append(personProfileMapping)
                .append("?AWS_ACCESS_KEY_ID=").append(objectStore.getS3Keys().getAccessKey()).append("&AWS_SECRET_ACCESS_KEY=")
                .append(objectStore.getS3Keys().getSecretKeys()).append("&AWS_ENDPOINT=").append(mercuryS3Properties.getDataplaneEndpoint()).append("' WITH chunk_rows='5000000'");
        String exportQuery = "EXPORT INTO CSV %s FROM select cnst.ol_prs_id,cnst.ol_prfl_id from %s cnst AS OF SYSTEM TIME '-1h'".formatted(sb, tableName);
        consentJdbcTemplate.execute((ConnectionCallback<Object>) connection -> {
            try (PreparedStatement statement = connection.prepareStatement(exportQuery)) {
                statement.execute();
            } catch (Exception e) {
                LOG.error("Exception occurred during export", e);
                throw e;
            }
            return null;
        });


        public void uploadCsvToS3(String csvFilePath) {
    LOG.info("Uploading CSV file to S3...");

    // Define S3 bucket name and object key
    String bucketName = mercuryS3Properties.getBucket();
    String objectKey = "uploads/" + Paths.get(csvFilePath).getFileName().toString(); // S3 file path

    // Create S3 client
    S3Client s3Client = S3Client.builder()
            .region(Region.of(mercuryS3Properties.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(objectStore.getS3Keys().getAccessKey(), 
                                               objectStore.getS3Keys().getSecretKeys())))
            .build();

    try {
        // Upload file
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(objectKey)
                        .build(),
                Paths.get(csvFilePath));

        LOG.info("CSV file successfully uploaded to S3: s3://{}/{}", bucketName, objectKey);
    } catch (S3Exception e) {
        LOG.error("S3 upload failed: {}", e.getMessage(), e);
    } finally {
        s3Client.close();
    }
}





public void uploadFileToS3(String filePath) throws IOException {
    if (isUnitTest) {
        return;
    }
    
    LOG.info("Uploading file to S3");
    
    String personProfileMapping = CDPConstants.prefix + FileNames.PERSON_PROFILE + 
                                  DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now());
    String s3UploadPath = CDPConstants.S3_PROTOCOL + mercuryS3Properties.getBucket() + 
                          "/" + personProfileMapping;
    
    // Read file content
    File file = new File(filePath);
    if (!file.exists()) {
        throw new FileNotFoundException("File not found: " + filePath);
    }
    
    try (InputStream inputStream = new FileInputStream(file)) {
        objectStore.uploadFile(s3UploadPath, inputStream, file.length());
    } catch (Exception e) {
        LOG.error("Exception occurred during file upload", e);
        throw e;
    }
    
    LOG.info("File successfully uploaded to S3: " + s3UploadPath);
}



public void putObject(final String key, final InputStream objectStream) throws IOException {
        final PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();
        final RequestBody requestBody = RequestBody.fromBytes(ByteStreams.toByteArray(objectStream));
        client.putObject(request, requestBody);
    }








    public void uploadFileToS3(String filePath) throws IOException {
    if (isUnitTest) {
        return;
    }
    
    LOG.info("Uploading file to S3");
    
    String personProfileMapping = CDPConstants.prefix + FileNames.PERSON_PROFILE + 
                                  DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now());
    String s3Key = personProfileMapping;
    
    // Read file content
    File file = new File(filePath);
    if (!file.exists()) {
        throw new FileNotFoundException("File not found: " + filePath);
    }
    
    try (InputStream inputStream = new FileInputStream(file)) {
        objectStore.putObject(s3Key, inputStream);
    } catch (Exception e) {
        LOG.error("Exception occurred during file upload", e);
        throw e;
    }
    
    LOG.info("File successfully uploaded to S3: " + s3Key);
}








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


if(clientId.matches("TTAX|JPMINTQBO100002|JPMINTMIN100001")){
        aggregatorId="INTUIT";
    }
    else{
        String[] clientIdArr = clientId.split(UNDER_SCORE);
        aggregatorId = clientIdArr[0];
    }


