/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.powsybl.commons.compress.ZipPackager;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public final class CgmesUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesUtils.class);

    public static final Set<String> NEEDED_PROFILES = new TreeSet<>(Arrays.asList("EQ", "SSH", "SV", "TP"));

    // We add sourcingActor "XX" for test purpose
    private static final Set<String> NEEDED_SOURCING_ACTORS = new TreeSet<>(Arrays.asList("REE", "REN", "RTEFRANCE", "REE-ES", "REN-PT", "RTEFRANCE-FR", "XX"));

    private static final String SV_MODEL_PART = "SV";

    private static final String EQ_MODEL_PART = "EQ";

    private  CgmesUtils() {
    }

  /*The file should have the following structure:
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_SV_<fileVersion>.zip
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_SSH_<fileVersion>.zip
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_TP_<fileVersion>.zip
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_EQ_<fileVersion>.zip
    <effectiveDateTime>__<sourcingActor>_EQ_<fileVersion>.zip (two underscores between <effectiveDateTime> and <sourcingActor>)
    where :
    <effectiveDateTime>: UTC datetime (YYYYMMDDTHHmmZ)
    <businessProcess>: String (YR, MO, WK, 2D, 1D, 23, …, 01 or RT)
    <sourcingActor>: String (REE, REN, RTEFRANCE, REE-ES, REN-PT or RTEFRANCE-FR)
    <modelPart>: String (EQ, TP, SSH or SV)
    <fileVersion>: three characters long positive integer number between 000 and 999. The most recent version has to be used
     */
    private static final String DOT_REGEX = "\\.";
    private static final String UNDERSCORE_REGEX = "\\_";

    public static boolean isValidProfileFileName(String filename) {
        if (filename.split(DOT_REGEX).length == 2) {
            String base = filename.split(DOT_REGEX)[0];
            String ext = filename.split(DOT_REGEX)[1];
            if (ext.equals("zip") && base.split(UNDERSCORE_REGEX).length == 5) {
                String[] parts = base.split(UNDERSCORE_REGEX);
                if (isValidModelPart(parts[3]) && isValidBusinessProcess(parts[1], parts[3]) && isValidSourcingActor(parts[2]) && isValidModelVersion(parts[4])) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isValidModelVersion(String version) {
        int v = Integer.parseInt(version);
        return version.length() == 3 && v > 0 && v < 1000;
    }

    private static boolean isValidSourcingActor(String sourcingActor) {
        return NEEDED_SOURCING_ACTORS.contains(sourcingActor);
    }

    private static boolean isValidBusinessProcess(String businessProcess, String modelPart) {
        return !businessProcess.isEmpty() || modelPart.equals(EQ_MODEL_PART);
    }

    private static boolean isValidModelPart(String modelPart) {
        return NEEDED_PROFILES.contains(modelPart);
    }

    public static boolean isSVFile(String filename) {
        String base = filename.split(DOT_REGEX)[0];
        String[] parts = base.split(UNDERSCORE_REGEX);
        return parts[3].equals(SV_MODEL_PART);
    }

    public static ZipInputStream getZipInputStream(byte[] compressedData) throws IOException {
        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(compressedData));
        zis.getNextEntry();
        return zis;
    }

    public static List<String> getDependenciesTreeUuids(String uuid, CgmesAssemblingLogger cgmesAssemblingLogger) {
        List<String> dependencies = cgmesAssemblingLogger.getDependencies(uuid);
        List<String> uuids = new ArrayList<>();
        if (dependencies == null || dependencies.isEmpty()) {
            return Arrays.asList(uuid);
        }
        uuids.add(uuid);
        for (String depUuid : dependencies) {
            uuids.addAll(getDependenciesTreeUuids(depUuid, cgmesAssemblingLogger));
        }
        return uuids;
    }

    public static TransferableFile prepareFinalZip(String filenameSV, Set<String> availableFileDependencies, Set<String> missingDependencies,
                                                   CgmesAssemblingLogger cgmesAssemblingLogger,
                                                   String casesDirectory, SftpConnection sftpConnection) throws IOException {
        ZipPackager emptyZipPackager = new ZipPackager();

        String cgmesFileName = filenameSV.replace("_" + SV_MODEL_PART, "");

        // Search for missing dependencies in the boundaries database table
        for (String depend : missingDependencies) {
            Pair<String, byte[]> boundary = cgmesAssemblingLogger.getBoundary(depend);
            if (boundary == null) {
                LOGGER.error("{} dependency not found in the boundaries database table", depend);
                return null;
            } else {
                LOGGER.info("assembling boundary file {} into CGMES {} file", boundary.getLeft(), cgmesFileName);
                emptyZipPackager.addBytes(boundary.getLeft(), boundary.getRight());
            }
        }

        // Get and add available files in the zip package
        List<String> filenames = availableFileDependencies.stream().map(e -> casesDirectory + "/" + e).collect(Collectors.toList());
        for (String s : filenames) {
            TransferableFile file = sftpConnection.getFile(s);
            LOGGER.info("assembling available file {} into CGMES {} file", file.getName(), cgmesFileName);
            emptyZipPackager.addBytes(file.getName().replace(".zip", ".xml"), getZipInputStream(file.getData()).readAllBytes());
        }

        return new TransferableFile(cgmesFileName, emptyZipPackager.toZipBytes());
    }
}
