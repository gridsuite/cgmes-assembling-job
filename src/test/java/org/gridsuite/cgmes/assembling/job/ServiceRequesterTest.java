/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import org.gridsuite.cgmes.assembling.job.dto.BoundaryInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockserver.model.HttpRequest.request;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@ExtendWith(MockServerExtension.class)
class ServiceRequesterTest {
    @Test
    void testCaseImportRequester(final MockServerClient mockServerClient) throws Exception {
        String fileData = "Case file content";
        CaseImportServiceRequester caseImportServiceRequester = new CaseImportServiceRequester("http://localhost:" + mockServerClient.getPort() + "/");

        TestUtils.expectRequestPost(mockServerClient, "/v1/cases/public", 200);
        assertTrue(caseImportServiceRequester.importCase(new TransferableFile("case.iidm", fileData.getBytes(UTF_8))));

        mockServerClient.clear(request());
        TestUtils.expectRequestPost(mockServerClient, "/v1/cases/public", 500);
        assertFalse(caseImportServiceRequester.importCase(new TransferableFile("case.iidm", fileData.getBytes(UTF_8))));
    }

    @Test
    void testCgmesBoundaryRequester(final MockServerClient mockServerClient) {
        CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester("http://localhost:" + mockServerClient.getPort() + "/");

        TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        BoundaryInfo res = cgmesBoundaryServiceRequester.getBoundary("urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358");
        assertEquals("titi.xml", res.getFilename());
        assertEquals("content1", new String(res.getBoundary(), UTF_8));
        assertEquals("urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", res.getId());

        mockServerClient.clear(request());
        TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
        assertNull(cgmesBoundaryServiceRequester.getBoundary("urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"));
    }

    @Test
    void testCgmesLastBoundariesRequester(final MockServerClient mockServerClient) {
        CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester("http://localhost:" + mockServerClient.getPort() + "/");

        TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/last", "[{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:11111111-2222-3333-4444-555555555555\",\"boundary\":\"content\"},{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\",\"boundary\":\"content2\"}]", 200);
        List<BoundaryInfo> res = cgmesBoundaryServiceRequester.getLastBoundaries();
        assertEquals(2, res.size());
        assertEquals("titi.xml", res.get(0).getFilename());
        assertEquals("tutu.xml", res.get(1).getFilename());
        assertEquals("urn:uuid:11111111-2222-3333-4444-555555555555", res.get(0).getId());
        assertEquals("urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", res.get(1).getId());
        assertEquals("content", new String(res.get(0).getBoundary(), UTF_8));
        assertEquals("content2", new String(res.get(1).getBoundary(), UTF_8));

        mockServerClient.clear(request());
        TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/last", "[]", 500);
        assertTrue(cgmesBoundaryServiceRequester.getLastBoundaries().isEmpty());
    }
}
