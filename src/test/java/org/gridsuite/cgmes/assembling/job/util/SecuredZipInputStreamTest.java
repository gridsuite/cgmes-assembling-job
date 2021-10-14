/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job.util;

import com.google.common.io.ByteStreams;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.Assert.*;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
public class SecuredZipInputStreamTest {
    @Test
    public void test() throws IOException {
        byte[] fileContent = ByteStreams.toByteArray(Objects.requireNonNull(getClass().getResourceAsStream("/MicroGridTestConfiguration_T4_BE_BB_Complete_v2.zip")));
        SecuredZipInputStream tooManyEntriesSecuredZis = new SecuredZipInputStream(new ByteArrayInputStream(fileContent), 3, 1000000000);
        assertTrue(assertThrows(IllegalStateException.class, () -> readZip(tooManyEntriesSecuredZis))
                    .getMessage().contains("Zip has too many entries."));

        SecuredZipInputStream tooBigSecuredZis = new SecuredZipInputStream(new ByteArrayInputStream(fileContent), 1000, 15000);
        assertTrue(assertThrows(IllegalStateException.class, () -> readZip(tooBigSecuredZis))
                .getMessage().contains("Zip size is too big."));

        SecuredZipInputStream okSecuredZis = new SecuredZipInputStream(new ByteArrayInputStream(fileContent), 1000, 1000000000);
        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(fileContent));
        assertEquals(readZip(zis), readZip(okSecuredZis));
    }

    public int readZip(ZipInputStream zis) throws IOException {
        ZipEntry entry = zis.getNextEntry();
        int readBytes = 0;
        while (entry != null) {
            readBytes += zis.readAllBytes().length;
            entry = zis.getNextEntry();
        }
        return readBytes;
    }
}
