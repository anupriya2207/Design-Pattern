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
