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
