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
import org.junit.*;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class ProfilesAcquisitionJobTest {

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
            assertEquals("testFile.iidm", cgmesAssemblingLogger.getFileNameByUuid("uuid", "my_sftp_server"));
            assertEquals("uuid", cgmesAssemblingLogger.getUuidByFileName("testFile.iidm", "my_sftp_server"));
            assertTrue(cgmesAssemblingLogger.isHandledFile("testFile.iidm", "my_sftp_server"));

            cgmesAssemblingLogger.logFileDependencies("uuid", Arrays.asList("uuid1", "uuid2"));
            assertEquals(2, cgmesAssemblingLogger.getDependencies("uuid").size(), 2);
        }
    }

    @Test
    public void testFtpAcquisition() throws IOException {
        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry("/cases"));
        fileSystem.add(new FileEntry("/cases/case1.iidm", "fake file content 1"));
        fileSystem.add(new FileEntry("/cases/case2.iidm", "fake file content 2"));

        FakeFtpServer fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.addUserAccount(new UserAccount("dummy_ftp", "dummy_ftp", "/"));
        fakeFtpServer.setFileSystem(fileSystem);
        fakeFtpServer.setServerControlPort(0);

        fakeFtpServer.start();

        String acquisitionServerUrl = "ftp://localhost:" + fakeFtpServer.getServerControlPort();
        try (AcquisitionServer acquisitionServer = new AcquisitionServer(acquisitionServerUrl, "dummy_ftp", "dummy_ftp")) {
            acquisitionServer.open();
            Map<String, String> retrievedFiles = acquisitionServer.listFiles("./cases");
            assertEquals(2, retrievedFiles.size());

            TransferableFile file1 = acquisitionServer.getFile("case1.iidm", acquisitionServerUrl + "/cases/case1.iidm");
            assertEquals("case1.iidm", file1.getName());
            assertEquals("fake file content 1", new String(file1.getData(), UTF_8));

            TransferableFile file2 = acquisitionServer.getFile("case2.iidm", acquisitionServerUrl + "/cases/case2.iidm");
            assertEquals("case2.iidm", file2.getName());
            assertEquals("fake file content 2", new String(file2.getData(), UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fakeFtpServer.stop();
        }
    }

    @Test
    public void testCaseImportRequester() throws IOException, InterruptedException {
        String fileData = "Case file content";
        CaseImportServiceRequester caseImportServiceRequester = new CaseImportServiceRequester("http://localhost:45385/");

        expectRequestCase("/v1/cases/public", 200);
        assertTrue(caseImportServiceRequester.importCase(new TransferableFile("case.iidm", fileData.getBytes(UTF_8))));

        mockServer.getClient().clear(request());
        expectRequestCase("/v1/cases/public", 500);
        assertFalse(caseImportServiceRequester.importCase(new TransferableFile("case.iidm", fileData.getBytes(UTF_8))));
    }

    @Test
    public void testCgmesBoundaryRequester() {
        CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester("http://localhost:55487/");

        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        Pair<String, byte[]> res = cgmesBoundaryServiceRequester.getBoundary("urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358");
        assertEquals("titi.xml", res.getLeft());
        assertEquals("content1", new String(res.getRight(), UTF_8));

        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
        assertNull(cgmesBoundaryServiceRequester.getBoundary("urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71"));
    }

    @Test
    public void testCgmesLastBoundariesRequester() {
        CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester("http://localhost:55487/");

        expectRequestBoundary("/v1/boundaries/last", "[{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:11111111-2222-3333-4444-555555555555\",\"boundary\":\"content\"},{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\",\"boundary\":\"content2\"}]", 200);
        Map<String, byte[]> res = cgmesBoundaryServiceRequester.getLastBoundaries();
        assertEquals(2, res.size());
        assertTrue(res.containsKey("titi.xml"));
        assertTrue(res.containsKey("tutu.xml"));
        assertEquals("content", new String(res.get("titi.xml"), UTF_8));
        assertEquals("content2", new String(res.get("tutu.xml"), UTF_8));

        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/last", null, 500);
        assertNull(cgmesBoundaryServiceRequester.getLastBoundaries());
    }

    private void expectRequestBoundary(String path, String response, Integer status) {
        mockServer.getClient().when(request().withMethod("GET").withPath(path),
                Times.exactly(1))
                .respond(response().withStatusCode(status)
                        .withBody(response));
    }

    private void expectRequestCase(String path, Integer status) {
        mockServer.getClient().when(request().withMethod("POST").withPath(path),
            Times.exactly(1))
            .respond(response().withStatusCode(status));
    }

    @Test
    public void testImportWithReferencedBoundaries() throws IOException {
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

        // 2 files on SFTP server (SV and SSH), 2 cases will be handled, but no import will be requested (missing dependencies)
        //
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        expectRequestBoundary("/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01", null, 500);
        ProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // No new files on SFTP server, no import requested (missing dependencies)
        //
        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        expectRequestBoundary("/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01", null, 500);
        ProfilesAcquisitionJob.main(args);
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // One new file on SFTP server (EQ), one new case will be handled, but still no import will be requested (missing dependencies)
        //
        try (InputStream isEQ = getClass().getResourceAsStream("/20191106T0930Z__XX_EQ_001.zip");
             BufferedInputStream bisEQ = new BufferedInputStream(isEQ);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z__XX_EQ_001.zip", bisEQ.readAllBytes());
        }

        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        expectRequestBoundary("/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01", null, 500);
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", "{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"content2\"}", 200);
        ProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // One new file on SFTP server (TP), one case import requested
        //
        try (InputStream isTP = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_TP_001.zip");
             BufferedInputStream bisTP = new BufferedInputStream(isTP);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_TP_001.zip", bisTP.readAllBytes());
        }

        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", "{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"content2\"}", 200);
        ProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // dependencies-strict-mode=true and not all referenced boundaries available
        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
        expectRequestCase("/v1/cases/public", 200);
        ProfilesAcquisitionJob.handle(Boolean.TRUE);
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // dependencies-strict-mode=false and all referenced boundaries available
        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"content1\"}", 200);
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", "{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"content2\"}", 200);
        expectRequestCase("/v1/cases/public", 200);
        ProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));
    }

    @Test
    public void testImportWithLastBoundaries() throws IOException {
        SFTP_SERVER_RULE.createDirectory("/cases");

        try (InputStream isSSH = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_SSH_001.zip");
             InputStream isSV = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_SV_001.zip");
             InputStream isTP = getClass().getResourceAsStream("/20191106T0930Z_1D_XX_TP_001.zip");
             InputStream isEQ = getClass().getResourceAsStream("/20191106T0930Z__XX_EQ_001.zip");

             BufferedInputStream bisSSH = new BufferedInputStream(isSSH);
             BufferedInputStream bisSV = new BufferedInputStream(isSV);
             BufferedInputStream bisTP = new BufferedInputStream(isTP);
             BufferedInputStream bisEQ = new BufferedInputStream(isEQ);) {
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_SSH_001.zip", bisSSH.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_SV_001.zip", bisSV.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z_1D_XX_TP_001.zip", bisTP.readAllBytes());
            SFTP_SERVER_RULE.putFile("/cases/20191106T0930Z__XX_EQ_001.zip", bisEQ.readAllBytes());
        }

        CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger();
        cgmesAssemblingLogger.connectDb("localhost", 9142);

        String[] args = null;

        // All individual profile files on SFTP server
        ProfilesAcquisitionJob.main(args);
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", "my_sftp_server"));
        assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", "my_sftp_server"));
        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // retry with import in case server available
        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", null, 500);
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
        expectRequestBoundary("/v1/boundaries/last", null, 500);
        expectRequestCase("/v1/cases/public", 200);
        ProfilesAcquisitionJob.main(args);

        assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", "my_sftp_server"));

        // retry with last boundaries and import in case server available
        mockServer.getClient().clear(request());
        expectRequestBoundary("/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", null, 500);
        expectRequestBoundary("/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
        expectRequestBoundary("/v1/boundaries/last", "[{\"filename\":\"titi.xml\",\"id\":\"urn:uuid:11111111-2222-3333-4444-555555555555\",\"boundary\":\"content\"},{\"filename\":\"tutu.xml\",\"id\":\"urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\",\"boundary\":\"content2\"}]", 200);
        expectRequestCase("/v1/cases/public", 200);
        ProfilesAcquisitionJob.main(args);

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

        ProfilesAcquisitionJob.main(args);

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

        String acquisitionServerUrl = "sftp://localhost:2222";
        try (AcquisitionServer acquisitionServer = new AcquisitionServer(acquisitionServerUrl, "dummy", "dummy")) {
            acquisitionServer.open();
            Map<String, String> retrievedFiles = acquisitionServer.listFiles("./cases");
            assertEquals(4, retrievedFiles.size());

            TransferableFile file1 = acquisitionServer.getFile("20200817T1705Z_1D_RTEFRANCE-FR_SV_002.zip", acquisitionServerUrl + "/cases/20200817T1705Z_1D_RTEFRANCE-FR_SV_002.zip");
            assertTrue(CgmesUtils.isValidProfileFileName(file1.getName()));
            assertEquals("20200817T1705Z_1D_RTEFRANCE-FR_SV_002.zip", file1.getName());
            assertEquals("fake file content 1", new String(file1.getData(), UTF_8));

            TransferableFile file2 = acquisitionServer.getFile("20200817T1705Z__RTEFRANCE-FR_EQ_002.zip", acquisitionServerUrl + "/cases/20200817T1705Z__RTEFRANCE-FR_EQ_002.zip");
            assertTrue(CgmesUtils.isValidProfileFileName(file2.getName()));
            assertEquals("20200817T1705Z__RTEFRANCE-FR_EQ_002.zip", file2.getName());
            assertEquals("fake file content 2", new String(file2.getData(), UTF_8));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
