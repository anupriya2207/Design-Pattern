response = Objects.requireNonNull(webClient
                    .get()
                    .uri(generateURI(request.getAppClientId()))
                    .headers(httpHeaders -> httpHeaders.putAll(getHttpHeaders(request)))
                    .retrieve()
                    .toEntity(UpdateAppStatusResponse.class)
                    .block()).getBody();
