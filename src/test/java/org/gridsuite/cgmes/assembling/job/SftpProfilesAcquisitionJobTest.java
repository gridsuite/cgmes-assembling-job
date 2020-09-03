/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.github.nosan.embedded.cassandra.api.cql.CqlDataSet;
import com.github.nosan.embedded.cassandra.junit4.test.CassandraRule;
import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class SftpProfilesAcquisitionJobTest {

    @ClassRule
    public static final CassandraRule CASSANDRA_RULE = new CassandraRule().withCassandraFactory(EmbeddedCassandraFactoryConfig.embeddedCassandraFactory())
                                                                          .withCqlDataSet(CqlDataSet.ofClasspaths("create_keyspace.cql", "cgmes_assembling.cql"));

    @ClassRule
    public static final FakeSftpServerRule SFTP_SERVER_RULE = new FakeSftpServerRule().addUser("dummy", "dummy").setPort(2222);

    @Rule
    public final MockServerRule mockServer = new MockServerRule(this, 45385, 55487);

    @Before
    public void setUp() throws IOException {
        CqlDataSet.ofClasspaths("truncate.cql").forEachStatement(CASSANDRA_RULE.getCassandraConnection()::execute);
        SFTP_SERVER_RULE.deleteAllFilesAndDirectories();
    }

    @After
    public void tearDown() throws IOException {
        CqlDataSet.ofClasspaths("truncate.cql").forEachStatement(CASSANDRA_RULE.getCassandraConnection()::execute);
        SFTP_SERVER_RULE.deleteAllFilesAndDirectories();
    }

    @Test
    public void historyLoggerTest() {
        try (CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger()) {
            cgmesAssemblingLogger.connectDb("localhost", 9142);
            assertFalse(cgmesAssemblingLogger.isHandledFile("testFile.iidm", "my_sftp_server"));
            cgmesAssemblingLogger.logFileAvailable("testFile.iidm", "uuid", "my_sftp_server", new Date());
            assertEquals(cgmesAssemblingLogger.getFileNameByUuid("uuid", "my_sftp_server"), "testFile.iidm");
            assertEquals(cgmesAssemblingLogger.getUuidByFileName("testFile.iidm", "my_sftp_server"), "uuid");
            assertTrue(cgmesAssemblingLogger.isHandledFile("testFile.iidm", "my_sftp_server"));

            cgmesAssemblingLogger.logFileDependencies("uuid", Arrays.asList("uuid1", "uuid2"));
            assertEquals(cgmesAssemblingLogger.getDependencies("uuid").size(), 2, 0);

        }
    }

    @Test
    public void testSftpConnection() throws IOException {

        SFTP_SERVER_RULE.createDirectory("/cases");
        SFTP_SERVER_RULE.putFile("/cases/case1.iidm", "fake file content 1", UTF_8);
        SFTP_SERVER_RULE.putFile("/cases/case2.iidm", "fake file content 2", UTF_8);

        try (SftpConnection sftpConnection = new SftpConnection()) {
            sftpConnection.open("localhost", SFTP_SERVER_RULE.getPort(), "dummy", "dummy");
            List<Path> retrievedFiles = sftpConnection.listFiles("./cases");
            assertEquals(2, retrievedFiles.size());

            TransferableFile file1 = sftpConnection.getFile("./cases/case1.iidm");
            assertEquals("case1.iidm", file1.getName());
            assertEquals("fake file content 1", new String(file1.getData(), UTF_8));

            TransferableFile file2 = sftpConnection.getFile("./cases/case2.iidm");
            assertEquals("case2.iidm", file2.getName());
            assertEquals("fake file content 2", new String(file2.getData(), UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCaseImportRequester() throws IOException, InterruptedException {
        CaseImportServiceRequester caseImportServiceRequester = new CaseImportServiceRequester("http://localhost:45385/");
        mockServer.getClient().when(request().withMethod("POST").withPath("/v1/cases/public"))
                .respond(response().withStatusCode(200));
        assertTrue(caseImportServiceRequester.importCase(new TransferableFile("case.iidm", new String("Case file content").getBytes(UTF_8))));
        mockServer.getClient().clear(request());
        mockServer.getClient().when(request().withMethod("POST").withPath("/v1/cases/public"))
                .respond(response().withStatusCode(500));
        assertFalse(caseImportServiceRequester.importCase(new TransferableFile("case.iidm", new String("Case file content").getBytes(UTF_8))));
    }

    @Test
    public void testCgmesBoundaryRequester() {
        CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester("http://localhost:55487/");
        String resExpected = "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358"))
                .respond(response().withStatusCode(200)
                        .withBody(resExpected));
        Pair<String, byte[]> res = cgmesBoundaryServiceRequester.getBoundary("urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358");
        assertEquals("titi.xml", res.getLeft());
        assertEquals("content", new String(res.getRight(), UTF_8));

        mockServer.getClient().clear(request());
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"))
                .respond(response().withStatusCode(500));
        assertNull(cgmesBoundaryServiceRequester.getBoundary("urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"));
    }

    @Test
    public void mainTest() throws IOException {
        SFTP_SERVER_RULE.createDirectory("/cases");

        try (InputStream isSSH = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_SSH_001.zip");
             InputStream isSV = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_SV_001.zip");
             BufferedInputStream bisSSH = new BufferedInputStream(isSSH);
             BufferedInputStream bisSV = new BufferedInputStream(isSV);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_SSH_001.zip", bisSSH.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_SV_001.zip", bisSV.readAllBytes());
        }

        CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger();
        cgmesAssemblingLogger.connectDb("localhost", 9142);

        String[] args = null;

        // 2 files on SFTP server (SV and SSH), 2 cases will be imported
        //
        String boundary1 = "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary1));
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01"),
                Times.exactly(1))
                .respond(response().withStatusCode(500));

        SftpProfilesAcquisitionJob.main(args);

        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // No new files on SFTP server, no import requested
        //
        mockServer.getClient().clear(request());
        boundary1 = "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary1));
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01"),
                Times.exactly(1))
                .respond(response().withStatusCode(500));

        SftpProfilesAcquisitionJob.main(args);
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // One new file on SFTP server (EQ), one case import requested
        //
        try (InputStream isEQ = getClass().getResourceAsStream("/20191106T0930Z__XX_EQ_001.zip");
             BufferedInputStream bisEQ = new BufferedInputStream(isEQ);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z__XX_EQ_001.zip", bisEQ.readAllBytes());
        }

        mockServer.getClient().clear(request());
        boundary1 = "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary1));
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01"),
                Times.exactly(1))
                .respond(response().withStatusCode(500));
        String boundary2 = "{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"content2\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary2));

        SftpProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // One new file on SFTP server (TP), one case import requested
        //
        try (InputStream isTP = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_TP_001.zip");
             BufferedInputStream bisTP = new BufferedInputStream(isTP);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_TP_001.zip", bisTP.readAllBytes());
        }

        mockServer.getClient().clear(request());
        boundary1 = "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary1));
        boundary2 = "{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"content2\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary2));

        SftpProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        boundary1 = "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary1));
        boundary2 = "{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"content2\"}";
        mockServer.getClient().when(request().withMethod("GET").withPath("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"),
                Times.exactly(1))
                .respond(response().withStatusCode(200)
                        .withBody(boundary2));

        mockServer.getClient().when(request().withMethod("POST").withPath("/v1/cases/public"),
                Times.exactly(1))
                .respond(response().withStatusCode(200));

        SftpProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));
    }

    @Test
    public void mainAssemblingTest() throws IOException {

        SFTP_SERVER_RULE.createDirectory("/cases");

        try (InputStream isSSH = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_SSH_001.zip");
             InputStream isSV = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_SV_001.zip");
             InputStream isTP = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_TP_001.zip");
             InputStream isEQ = getClass().getResourceAsStream("/20191106T0930Z__XX_EQ_001.zip");

             BufferedInputStream bisSV = new BufferedInputStream(isSV);
             BufferedInputStream bisSSH = new BufferedInputStream(isSSH);
             BufferedInputStream bisTP = new BufferedInputStream(isTP);
             BufferedInputStream bisEQ = new BufferedInputStream(isEQ);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_TP_001.zip", bisTP.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z__XX_EQ_001.zip", bisEQ.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_SV_001.zip", bisSV.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_SSH_001.zip", bisSSH.readAllBytes());
        }

        CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger();
        cgmesAssemblingLogger.connectDb("localhost", 9142);

        String[] args = null;

        SftpProfilesAcquisitionJob.main(args);

        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", "my_sftp_server"));
    }

    @Test
    public void testAcceptedFilesConnection() throws IOException {

        SFTP_SERVER_RULE.createDirectory("/cases");
        SFTP_SERVER_RULE.putFile("/cases/20200817T1705Z_1D_RTEFRANCE-FR_SV_002.zip", "fake file content 1", UTF_8);
        SFTP_SERVER_RULE.putFile("/cases/20200817T1705Z__RTEFRANCE-FR_EQ_002.zip", "fake file content 2", UTF_8);
        SFTP_SERVER_RULE.putFile("/cases/20200817T1705Z_1D_RTEFRANCE-FR_SSH_002.zip", "fake file content 3", UTF_8);
        SFTP_SERVER_RULE.putFile("/cases/20200817T1705Z_1D_RTEFRANCE-FR_TP_002.zip", "fake file content 4", UTF_8);

        try (SftpConnection sftpConnection = new SftpConnection()) {
            sftpConnection.open("localhost", SFTP_SERVER_RULE.getPort(), "dummy", "dummy");
            List<Path> retrievedFiles = sftpConnection.listFiles("./cases");
            assertEquals(4, retrievedFiles.size());

            TransferableFile file1 = sftpConnection.getFile("./cases/20200817T1705Z_1D_RTEFRANCE-FR_SV_002.zip");
            assertTrue(CgmesUtils.isValidProfileFileName(file1.getName()));
            assertEquals("20200817T1705Z_1D_RTEFRANCE-FR_SV_002.zip", file1.getName());
            assertEquals("fake file content 1", new String(file1.getData(), UTF_8));

            TransferableFile file2 = sftpConnection.getFile("./cases/20200817T1705Z__RTEFRANCE-FR_EQ_002.zip");
            assertTrue(CgmesUtils.isValidProfileFileName(file2.getName()));
            assertEquals("20200817T1705Z__RTEFRANCE-FR_EQ_002.zip", file2.getName());
            assertEquals("fake file content 2", new String(file2.getData(), UTF_8));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
