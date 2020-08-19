/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.powsybl.commons.compress.ZipPackager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 */
public final class CgmesUtils {

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
        List<String>  sourcingActors = Arrays.asList("REE", "REN", "RTEFRANCE", "REE-ES", "REN-PT", "RTEFRANCE-FR", "NG");
        return sourcingActors.contains(sourcingActor);
    }

    private static boolean isValidBusinessProcess(String businessProcess, String modelPart) {
        return !businessProcess.isEmpty() || modelPart.equals("EQ");
    }

    private static boolean isValidModelPart(String modelPart) {
        List<String> modelParts = Arrays.asList("SSH", "EQ", "TP", "SV");
        return modelParts.contains(modelPart);
    }

    public static boolean isSVFile(String filename) {
        String base = filename.split(DOT_REGEX)[0];
        String[] parts = base.split(UNDERSCORE_REGEX);
        return parts[3].equals("SV");
    }

    public static ZipInputStream getZipInputStream(byte[] compressedData) throws IOException {
        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(compressedData));
        zis.getNextEntry();
        return zis;
    }

    public static boolean isDependenciesTreeResolved(String uuid, CgmesAssemblingLogger cgmesAssemblingLogger) {
        List<String> dependencies = cgmesAssemblingLogger.getDependencies(uuid);
        if (dependencies == null) {
            return false;
        }
        if (dependencies.isEmpty()) {
            return true;
        }
        for (String depUuid : dependencies) {
            if (!isDependenciesTreeResolved(depUuid, cgmesAssemblingLogger)) {
                return false;
            }
        }
        return true;
    }

    public static List<String> getDependenciesTreeUuids(String uuid, CgmesAssemblingLogger cgmesAssemblingLogger) {
        List<String> dependencies = cgmesAssemblingLogger.getDependencies(uuid);
        List<String> uuids = new ArrayList<>();
        if (dependencies.isEmpty()) {
            return Arrays.asList(uuid);
        }
        for (String depUuid : dependencies) {
            uuids.addAll(getDependenciesTreeUuids(depUuid, cgmesAssemblingLogger));
        }
        return uuids;
    }

    public static TransferableFile prepareFinalZip(String filenameSV, String origin, CgmesAssemblingLogger cgmesAssemblingLogger,
                                                   String casesDirectory, SftpConnection sftpConnection) throws IOException {
        String uuid = cgmesAssemblingLogger.getUuidByFileName(filenameSV, origin);

        List<String> filenames = getDependenciesTreeUuids(uuid, cgmesAssemblingLogger)
                .stream().map(e -> casesDirectory + "/" + cgmesAssemblingLogger.getFileNameByUuid(e, origin))
                .collect(Collectors.toList());

        ZipPackager emptyZipPackager = new ZipPackager();

        for (String s : filenames) {
            TransferableFile file = sftpConnection.getFile(s);
            emptyZipPackager.addBytes(file.getName(), file.getData());
        }

        return new TransferableFile(filenameSV.replace("_SV", ""), emptyZipPackager.toZipBytes());
    }
}
