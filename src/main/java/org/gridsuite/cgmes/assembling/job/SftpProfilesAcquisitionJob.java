/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;
import org.gridsuite.cgmes.assembling.fullmodel.FullModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import java.util.zip.ZipInputStream;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public final class SftpProfilesAcquisitionJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(SftpProfilesAcquisitionJob.class);

    private SftpProfilesAcquisitionJob() {
    }

    public static void main(String... args) {

        PlatformConfig platformConfig = PlatformConfig.defaultConfig();

        ModuleConfig moduleConfigSftpServer = platformConfig.getModuleConfig("sftp-server");
        ModuleConfig moduleConfigCassandra = platformConfig.getModuleConfig("cassandra");
        ModuleConfig moduleConfigCaseServer = platformConfig.getModuleConfig("case-server");

        final CaseImportServiceRequester caseImportServiceRequester = new CaseImportServiceRequester(moduleConfigCaseServer.getStringProperty("url"));

        try (SftpConnection sftpConnection = new SftpConnection();
             CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger()) {

            sftpConnection.open(moduleConfigSftpServer.getStringProperty("hostname"),
                                moduleConfigSftpServer.getIntProperty("port", 22),
                                moduleConfigSftpServer.getStringProperty("username"),
                                moduleConfigSftpServer.getStringProperty("password"));

            cgmesAssemblingLogger.connectDb(moduleConfigCassandra.getStringProperty("contact-points"), moduleConfigCassandra.getIntProperty("port"));

            String casesDirectory = moduleConfigSftpServer.getStringProperty("cases-directory");
            String sftpServerLabel = moduleConfigSftpServer.getStringProperty("label");

            // Get valid zip files
            List<Path> filesToAcquire = sftpConnection.listFiles(casesDirectory)
                    .stream()
                    .filter(file -> CgmesUtils.isValidProfileFileName(file.getFileName().toString()))
                    .collect(Collectors.toList());
            LOGGER.info("{} valid files found on SFTP server", filesToAcquire.size());

            // Get SV files
            List<Path> filesSV = filesToAcquire
                    .stream()
                    .filter(file -> CgmesUtils.isSVFile(file.getFileName().toString()))
                    .collect(Collectors.toList());
            LOGGER.info("{} valid SV files found on SFTP server", filesSV.size());

            List<Path> filesHandled = new ArrayList<>();
            List<Path> filesAlreadyHandled = new ArrayList<>();
            List<Path> filesImportingFailed = new ArrayList<>();
            List<Path> filesSuccessfullyImported = new ArrayList<>();
            List<Path> filesAlreadyImported = new ArrayList<>();

            for (Path file : filesToAcquire) {
                if (!cgmesAssemblingLogger.isHandledFile(file.getFileName().toString(), sftpServerLabel)) {
                    LOGGER.info("Handling file '{}'...", file);
                    // Download the file
                    TransferableFile acquiredFile = sftpConnection.getFile(file.toString());

                    try (ZipInputStream zipInputStream = CgmesUtils.getZipInputStream(acquiredFile.getData());
                         Reader reader = new InputStreamReader(zipInputStream)) {
                        FullModel fullModel = FullModel.parse(reader);
                        cgmesAssemblingLogger.logFileAvailable(file.getFileName().toString(), fullModel.getId(), sftpServerLabel, new Date());
                        cgmesAssemblingLogger.logFileDependencies(fullModel.getId(), fullModel.getDependentOn());
                    }
                    filesHandled.add(file);
                } else {
                    filesAlreadyHandled.add(file);
                }
            }

            for (Path file : filesSV) {
                if (!cgmesAssemblingLogger.isImportedFile(file.getFileName().toString(), sftpServerLabel)) {
                    LOGGER.info("SV file '{}'...", file);
                    String uuid = cgmesAssemblingLogger.getUuidByFileName(file.getFileName().toString(), sftpServerLabel);

                    // Identify available and missing file dependencies
                    List<String> dependencies = CgmesUtils.getDependenciesTreeUuids(uuid, cgmesAssemblingLogger);
                    Set<String> availableFileDependencies = new HashSet<>();
                    Set<String> missingDependencies = new HashSet<>();
                    for (String dependUuid : dependencies) {
                        String dependFileName = cgmesAssemblingLogger.getFileNameByUuid(dependUuid, sftpServerLabel);
                        if (dependFileName != null) {
                            availableFileDependencies.add(dependFileName);
                        } else {
                            missingDependencies.add(dependUuid);
                        }
                    }

                    // Assembling profiles
                    TransferableFile assembledFile = CgmesUtils.prepareFinalZip(file.getFileName().toString(), availableFileDependencies,
                            missingDependencies, cgmesAssemblingLogger, casesDirectory, sftpConnection);

                    if (assembledFile != null) {
                        // Import assembled file in the case server
                        boolean importOk = caseImportServiceRequester.importCase(assembledFile);
                        if (!importOk) {
                            filesImportingFailed.add(file);
                        } else {
                            filesSuccessfullyImported.add(file);
                            cgmesAssemblingLogger.logFileImported(file.getFileName().toString(), sftpServerLabel, new Date());
                        }
                    } else {
                        LOGGER.error("{} file's dependencies are not resolved yet", file.getFileName().toString());
                    }
                } else {
                    filesAlreadyImported.add(file);
                }
            }

            LOGGER.info("===== JOB EXECUTION SUMMARY =====");
            LOGGER.info("{} files already handled", filesAlreadyHandled.size());
            LOGGER.info("{} files successfully handled", filesHandled.size());
            filesHandled.forEach(f -> LOGGER.info("File '{}' successfully handled", f));

            LOGGER.info("{} files import succeeded", filesSuccessfullyImported.size());
            filesSuccessfullyImported.forEach(f -> LOGGER.info("Assembled files with  '{}' file successfully imported !!", f));
            LOGGER.info("{} files import failed", filesImportingFailed.size());
            filesImportingFailed.forEach(f -> LOGGER.info("Assembled files with  '{}' file import failed !!", f));
            LOGGER.info("{} files already imported", filesAlreadyImported.size());
            filesAlreadyImported.forEach(f -> LOGGER.info("Assembled files with  '{}' file  already imported !!", f));
            LOGGER.info("=================================");
        } catch (Exception exc) {
            LOGGER.error("Job execution error: {}", exc.getMessage());
        }
    }
}
