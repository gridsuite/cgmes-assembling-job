/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import org.gridsuite.cgmes.assembling.job.dto.BoundaryInfo;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class CgmesBoundaryServiceRequester {
    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesBoundaryServiceRequester.class);
    private static final String API_VERSION = "v1";
    private final String serviceUrl;
    private final HttpClient httpClient;
    private static final String ID_KEY = "id";
    private static final String FILE_NAME_KEY = "filename";
    private static final String BOUNDARY_KEY = "boundary";

    public CgmesBoundaryServiceRequester(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        httpClient = HttpClient.newHttpClient();
    }

    public BoundaryInfo getBoundary(String boundaryId) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(serviceUrl + API_VERSION + "/boundaries/" + boundaryId))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            LOGGER.info("Cgmes boundary server response status: {}", response.statusCode());

            if (response.statusCode() == 200) {
                String json = response.body();
                JSONObject obj = new JSONObject(json);
                return new BoundaryInfo(obj.getString(ID_KEY), obj.getString(FILE_NAME_KEY), obj.getString(BOUNDARY_KEY).getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            LOGGER.error("I/O Error while getting boundary with id {}", boundaryId);
        } catch (InterruptedException e) {
            LOGGER.error("Interruption when getting boundary with id {}", boundaryId);
            Thread.currentThread().interrupt();
        }
        return null;
    }

    public List<BoundaryInfo> getLastBoundaries() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serviceUrl + API_VERSION + "/boundaries/last"))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            LOGGER.info("Cgmes boundary server response status: {}", response.statusCode());

            if (response.statusCode() == 200) {
                String json = response.body();

                List<BoundaryInfo> result = new ArrayList<>();
                JSONArray array = new JSONArray(json);
                for (int i = 0; i < array.length(); i++) {
                    JSONObject obj = array.getJSONObject(i);
                    result.add(new BoundaryInfo(obj.getString(ID_KEY), obj.getString(FILE_NAME_KEY), obj.getString(BOUNDARY_KEY).getBytes(StandardCharsets.UTF_8)));
                }
                return result;
            }
        } catch (IOException e) {
            LOGGER.error("I/O Error while getting last boundaries");
        } catch (InterruptedException e) {
            LOGGER.error("Interruption when getting last boundaries");
            Thread.currentThread().interrupt();
        }
        return Collections.emptyList();
    }
}
