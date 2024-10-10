package org.gridsuite.cgmes.assembling.job;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CgmesUtilsTest {
    @Test
    void testGetValidProfileFileName() {
        final Set<String> authorizedSourcingActors = Set.of("XX");
        final Set<String> authorizedBusinessProcesses = Set.of("1D");

        assertEquals("SSH", CgmesUtils.getValidProfileFileName("20191106T0930Z_1D_XX_SSH_001.zip", authorizedSourcingActors, authorizedBusinessProcesses));
        assertNull(CgmesUtils.getValidProfileFileName("20191106T0930Z_1D_XX_SSH_1002.zip", authorizedSourcingActors, authorizedBusinessProcesses));
        assertNull(CgmesUtils.getValidProfileFileName("20191106T0930Z_1D_YY_SSH_001.zip", authorizedSourcingActors, authorizedBusinessProcesses));
        assertNull(CgmesUtils.getValidProfileFileName("20191106T0930Z_6D_XX_SSH_001.zip", authorizedSourcingActors, authorizedBusinessProcesses));
        assertNull(CgmesUtils.getValidProfileFileName("20191106T0930Z_1D_XX_SSH_001.xml", authorizedSourcingActors, authorizedBusinessProcesses));
        assertNull(CgmesUtils.getValidProfileFileName("20191106T0930Z_1D_XX_SSH_abc.zip", authorizedSourcingActors, authorizedBusinessProcesses));
    }
}
