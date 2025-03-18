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
