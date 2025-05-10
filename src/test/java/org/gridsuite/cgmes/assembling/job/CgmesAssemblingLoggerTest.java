package org.gridsuite.cgmes.assembling.job;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

@JdbcTest
class CgmesAssemblingLoggerTest {
    @Autowired private DataSource dataSource;
    @MockBean private ProfilesAcquisitionJob runner; //we don't want the runner to run during tests

    @Test
    void historyLoggerTest() {
        try (final CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
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
    void testLogDependencies() {
        assertThrows(RuntimeException.class, () -> {
            try (final CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
                cgmesAssemblingLogger.logFileDependencies("uuid", null);
            }
        });
    }
}
