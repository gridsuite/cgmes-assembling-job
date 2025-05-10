package org.gridsuite.cgmes.assembling.job;

import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.lang.NonNull;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.sql.PreparedStatement;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.model.HttpRequest.request;

@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
@ExtendWith(MockServerExtension.class)
@SpringBootTest(classes = {ProfilesAcquisitionJob.class})
class ProfilesAcquisitionJobTest {
    @Autowired private DataSource dataSource;
    @MockBean private ProfilesAcquisitionJob cliRunner; //we prevent the cli runner to run before tests
    private ProfilesAcquisitionJob profilesAcquisitionJob;

    @BeforeEach
    void setUp() {
        profilesAcquisitionJob = new ProfilesAcquisitionJob(dataSource);
    }

    @Test
    void testImportWithReferencedBoundaries(final MockServerClient mockServerClient) throws Exception {
        TestUtils.withSftp(server -> {
            putFile(server, "/20191106T0930Z_1D_XX_SSH_001.zip");
            putFile(server, "/20191106T0930Z_1D_XX_SV_001.zip");
            final String boundary1Content = StringEscapeUtils.escapeJava(IOUtils.toString(getClass().getResourceAsStream("/referenced_eqbd.xml"), UTF_8));
            final String boundary2Content = StringEscapeUtils.escapeJava(IOUtils.toString(getClass().getResourceAsStream("/referenced_tpbd.xml"), UTF_8));

            // 2 files on SFTP server (SV and SSH), 2 cases will be handled, but no import will be requested (missing dependencies)
            TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
            TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
            TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"referenced_eqbd.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"" + boundary1Content + "\"}", 200);
            TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01", null, 500);
            runJob(profilesAcquisitionJob, server, mockServerClient, false);
            try (CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));
                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // No new files on SFTP server, no import requested (missing dependencies)
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"referenced_eqbd.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"" + boundary1Content + "\"}", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01", null, 500);
                runJob(profilesAcquisitionJob, server, mockServerClient, false);
                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // One new file on SFTP server (EQ), one new case will be handled, but still no import will be requested (missing dependencies)
                putFile(server, "/20191106T0930Z__XX_EQ_001.zip");
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"referenced_eqbd.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"" + boundary1Content + "\"}", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:25b861c6-3e06-4fa1-bb56-592330202c01", null, 500);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", "{\"filename\":\"referenced_tpbd.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"" + boundary2Content + "\"}", 200);
                runJob(profilesAcquisitionJob, server, mockServerClient, false);
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", TestUtils.SFTP_LABEL));
                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // One new file on SFTP server (TP), one case import requested
                putFile(server, "/20191106T0930Z_1D_XX_TP_001.zip");
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"referenced_eqbd.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"" + boundary1Content + "\"}", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", "{\"filename\":\"referenced_tpbd.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"" + boundary2Content + "\"}", 200);
                runJob(profilesAcquisitionJob, server, mockServerClient, false);
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", TestUtils.SFTP_LABEL));
                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // dependencies-strict-mode=true and not all referenced boundaries available
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"referenced_eqbd.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"" + boundary1Content + "\"}", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
                TestUtils.expectRequestPost(mockServerClient, "/v1/cases/public", 200);
                runJob(profilesAcquisitionJob, server, mockServerClient, true);
                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // dependencies-strict-mode=false and all referenced boundaries available, and expect import case
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", "{\"filename\":\"referenced_eqbd.xml\",\"id\":\"urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358\",\"boundary\":\"" + boundary1Content + "\"}", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", "{\"filename\":\"referenced_tpbd.xml\",\"id\":\"urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71\",\"boundary\":\"" + boundary2Content + "\"}", 200);
                TestUtils.expectRequestPost(mockServerClient, "/v1/cases/public", 200);
                runJob(profilesAcquisitionJob, server, mockServerClient, false);
                assertTrue(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));
            }
        });
    }

    @Test
    void testImportWithLastBoundaries(final MockServerClient mockServerClient) throws Exception {
        TestUtils.withSftp(server -> {
            putFile(server, "/20191106T0930Z_1D_XX_SSH_001.zip");
            putFile(server, "/20191106T0930Z_1D_XX_SV_001.zip");
            putFile(server, "/20191106T0930Z_1D_XX_TP_001.zip");
            putFile(server, "/20191106T0930Z__XX_EQ_001.zip");

            // All individual profile files on SFTP server, no boundaries available
            TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
            TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
            runJob(profilesAcquisitionJob, server, mockServerClient, false);
            try (CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", TestUtils.SFTP_LABEL));
                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // retry with import in case server available and referenced boundaries not available
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", null, 500);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/last", "[]", 500);
                TestUtils.expectRequestPost(mockServerClient, "/v1/cases/public", 200);
                runJob(profilesAcquisitionJob, server, mockServerClient, false);

                assertFalse(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));

                // retry with last boundaries and import in case server available
                mockServerClient.clear(request());
                TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:f1582c44-d9e2-4ea0-afdc-dba189ab4358", null, 500);
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/urn:uuid:3e3f7738-aab9-4284-a965-71d5cd151f71", null, 500);
                final String lastBoundary1Content = StringEscapeUtils.escapeJava(IOUtils.toString(getClass().getResourceAsStream("/last_eqbd.xml"), UTF_8));
                final String lastBoundary2Content = StringEscapeUtils.escapeJava(IOUtils.toString(getClass().getResourceAsStream("/last_tpbd.xml"), UTF_8));
                TestUtils.expectRequestGet(mockServerClient, "/v1/boundaries/last", "[{\"filename\":\"last_eqbd.xml\",\"id\":\"urn:uuid:11111111-2222-3333-4444-555555555555\",\"boundary\":\"" + lastBoundary1Content + "\"},{\"filename\":\"last_tpbd.xml\",\"id\":\"urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\",\"boundary\":\"" + lastBoundary2Content + "\"}]", 200);
                TestUtils.expectRequestPost(mockServerClient, "/v1/cases/public", 200);
                runJob(profilesAcquisitionJob, server, mockServerClient, false);

                assertTrue(cgmesAssemblingLogger.isImportedFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));
            }
        });
    }

    @Test
    void mainAssemblingTest(final MockServerClient mockServerClient) throws Exception {
        TestUtils.withSftp(server -> {
            putFile(server, "/20191106T0930Z_1D_XX_SSH_001.zip");
            putFile(server, "/20191106T0930Z_1D_XX_SV_001.zip");
            putFile(server, "/20191106T0930Z_1D_XX_TP_001.zip");
            putFile(server, "/20191106T0930Z__XX_EQ_001.zip");

            TestUtils.expectRequestGet(mockServerClient, "/v1/tsos", "[\"XX\"]", 200);
            TestUtils.expectRequestGet(mockServerClient, "/v1/business-processes", "[\"1D\"]", 200);
            runJob(profilesAcquisitionJob, server, mockServerClient, false);

            try (final CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SSH_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_SV_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z_1D_XX_TP_001.zip", TestUtils.SFTP_LABEL));
                assertTrue(cgmesAssemblingLogger.isHandledFile("20191106T0930Z__XX_EQ_001.zip", TestUtils.SFTP_LABEL));
            }
        });
    }

    private static void putFile(FakeSftpServer server, String filepath) throws Exception {
        try (final BufferedInputStream bisEQ = new BufferedInputStream(ProfilesAcquisitionJobTest.class.getResourceAsStream(filepath))) {
            server.putFile("cases" + filepath, bisEQ.readAllBytes());
        }
    }

    private static void runJob(@NonNull final ProfilesAcquisitionJob jobRunner, @NonNull final FakeSftpServer sftpServer, @NonNull final MockServerClient mockServerClient, final boolean depsStrictMode) {
        jobRunner.handle(
            depsStrictMode,
            "http://localhost:" + mockServerClient.getPort() + "/", //random free port
            "http://localhost:" + mockServerClient.getPort() + "/",
            "dummy", //server.addUser(...)
            "dummy",
            "sftp://localhost:" + sftpServer.getPort(),
            "./cases", //server.createDirectory(...)
            TestUtils.SFTP_LABEL
        );
    }

    @AfterEach
    void cleanDB() throws Exception {
        try (final CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
            for (final String table : List.of("handled_files", "imported_files", "handled_files_dependencies")) {
                try (final PreparedStatement truncateStatement = cgmesAssemblingLogger.getConnection().prepareStatement("TRUNCATE TABLE " + table)) {
                    truncateStatement.executeUpdate();
                }
            }
        }
    }
}
