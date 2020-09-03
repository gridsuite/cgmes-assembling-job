/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class CgmesBoundaryServiceRequester {

    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesBoundaryServiceRequester.class);

    private static final String API_VERSION = "v1";

    private final String serviceUrl;

    private final HttpClient httpClient;

    public CgmesBoundaryServiceRequester(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        httpClient = HttpClient.newHttpClient();
    }

    public Pair<String, byte[]> getBoundary(String boundaryId) {

        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serviceUrl + API_VERSION + "/boundaries/" + boundaryId))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            LOGGER.info("Cgmes boundary server response status: {}", response.statusCode());

            if (response.statusCode() == 200) {
                String json = response.body();
                LOGGER.info("Http request response body: {}", json);

                JSONObject obj = new JSONObject(json);
                String boundaryXml = obj.getString("boundary");
                String filename = obj.getString("filename");

                return Pair.of(filename, boundaryXml.getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            LOGGER.error("I/O Error while getting boundary with id {}", boundaryId);
        } catch (InterruptedException e) {
            LOGGER.error("Interruption when getting boundary with id {}", boundaryId);
            Thread.currentThread().interrupt();
        }

        return null;
    }
}
