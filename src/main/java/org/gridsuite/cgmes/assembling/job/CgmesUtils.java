/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.powsybl.commons.compress.ZipPackager;
import org.gridsuite.cgmes.assembling.job.dto.BoundaryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public final class CgmesUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesUtils.class);
    private static final String SV_MODEL_PART = "SV";
    private static final String EQ_MODEL_PART = "EQ";
    private static final String SSH_MODEL_PART = "SSH";
    private static final String TP_MODEL_PART = "TP";
    private static final Set<String> NEEDED_PROFILES = new TreeSet<>(Arrays.asList(EQ_MODEL_PART, SSH_MODEL_PART, SV_MODEL_PART, TP_MODEL_PART));
    private static final String DOT_REGEX = "\\.";
    private static final String UNDERSCORE_REGEX = "_";

    private CgmesUtils() {
    }

  /*The file should have the following structure:
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_SV_<fileVersion>.zip
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_SSH_<fileVersion>.zip
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_TP_<fileVersion>.zip
    <effectiveDateTime>_<businessProcess>_<sourcingActor>_EQ_<fileVersion>.zip
    <effectiveDateTime>__<sourcingActor>_EQ_<fileVersion>.zip (two underscores between <effectiveDateTime> and <sourcingActor>)
    where :
    <effectiveDateTime>: UTC datetime (YYYYMMDDTHHmmZ)
    <businessProcess>: String (YR, MO, WK, 2D, 1D, 23, ..., 01, RT, ....)
    <sourcingActor>: String (REE, REN, RTEFRANCE, ....)
    <modelPart>: String (EQ, TP, SSH or SV)
    <fileVersion>: three characters long positive integer number between 000 and 999. The most recent version has to be used
     */
    public static String getValidProfileFileName(String filename, Set<String> authorizedSourcingActors, Set<String> authorizedBusinessProcesses) {
        if (filename.split(DOT_REGEX).length == 2) {
            String base = filename.split(DOT_REGEX)[0];
            String ext = filename.split(DOT_REGEX)[1];
            if (ext.equals("zip") && base.split(UNDERSCORE_REGEX).length == 5) {
                String[] parts = base.split(UNDERSCORE_REGEX);
                if (isValidModelPart(parts[3]) && isValidBusinessProcess(parts[1], parts[3], authorizedBusinessProcesses) &&
                    isValidSourcingActor(parts[2], authorizedSourcingActors) && isValidModelVersion(parts[4])) {
                    return parts[3];
                }
            }
        }
        return null;
    }

    public static boolean isValidProfileFileName(String filename, Set<String> authorizedSourcingActors, Set<String> authorizedBusinessProcesses) {
        return getValidProfileFileName(filename, authorizedSourcingActors, authorizedBusinessProcesses) != null;
    }

    private static boolean isValidModelVersion(String version) {
        try {
            int v = Integer.parseInt(version);
            return version.length() == 3 && v > 0 && v < 1000;
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid model version {}", version);
            return false;
        }
    }

    private static boolean isValidSourcingActor(String sourcingActor, Set<String> authorizedSourcingActors) {
        return authorizedSourcingActors.contains(sourcingActor);
    }

    private static boolean isValidBusinessProcess(String businessProcess, String modelPart, Set<String> authorizedBusinessProcesses) {
        return businessProcess.isEmpty() ? modelPart.equals(EQ_MODEL_PART) : authorizedBusinessProcesses.contains(businessProcess);
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

    public static TransferableFile prepareFinalZip(String filenameSV, Map<String, String> availableFileDependencies, Set<String> missingDependencies,
                                                   AcquisitionServer acquisitionServer, CgmesBoundaryServiceRequester boundaryServiceRequester,
                                                   boolean dependenciesStrictMode,
                                                   Set<String> authorizedTsos, Set<String> authorizedBusinessProcesses) throws IOException {
        // test if all needed individual profiles are available
        Set<String> availableProfiles = availableFileDependencies.keySet().stream().map(d -> CgmesUtils.getValidProfileFileName(d, authorizedTsos, authorizedBusinessProcesses)).collect(Collectors.toSet());
        if (!availableProfiles.equals(NEEDED_PROFILES)) {
            return null;
        }

        ZipPackager emptyZipPackager = new ZipPackager();

        String cgmesFileName = filenameSV.replace("_" + SV_MODEL_PART, "");

        // Search for missing referenced dependencies in the boundaries database table
        List<BoundaryInfo> boundaries = new ArrayList<>();
        for (String depend : missingDependencies) {
            BoundaryInfo boundary = boundaryServiceRequester.getBoundary(depend);
            if (boundary == null) {
                LOGGER.warn("{} referenced dependency not found in cgmes boundary server", depend);
                if (dependenciesStrictMode) {
                    return null;
                }
            } else {
                boundaries.add(boundary);
            }
        }

        // if at least one referenced boundary is missing, we use the last boundaries in the database
        if (boundaries.size() < 2) {
            boundaries.clear();
            boundaries.addAll(boundaryServiceRequester.getLastBoundaries());
        }
        if (boundaries.size() < 2) {
            LOGGER.warn("No boundaries found in cgmes boundary server");
            return null;
        }

        boundaries.forEach(boundary -> {
            LOGGER.info("assembling boundary file {} with uuid {} into CGMES {} file", boundary.getFilename(), boundary.getId(), cgmesFileName);
            emptyZipPackager.addBytes(boundary.getFilename(), boundary.getBoundary());
        });

        // Get and add available individual profile files in the zip package
        for (Map.Entry<String, String> availableFile : availableFileDependencies.entrySet()) {
            TransferableFile file = acquisitionServer.getFile(availableFile.getKey(), availableFile.getValue());
            LOGGER.info("assembling available file {} into CGMES {} file", file.getName(), cgmesFileName);
            emptyZipPackager.addBytes(file.getName().replace(".zip", ".xml"), getZipInputStream(file.getData()).readAllBytes());
        }

        return new TransferableFile(cgmesFileName, emptyZipPackager.toZipBytes());
    }
}
