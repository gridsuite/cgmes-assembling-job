/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.powsybl.cgmes.model.FullModel;
import com.powsybl.commons.PowsyblException;
import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.lang.Nullable;

import javax.sql.DataSource;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@SpringBootApplication
@AllArgsConstructor
public class ProfilesAcquisitionJob implements CommandLineRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProfilesAcquisitionJob.class);

    private final DataSource dataSource;

    public static void main(String... args) {
        SpringApplication.run(ProfilesAcquisitionJob.class, args);
    }

    @Override
    public void run(String... args) {
        handle(null);
    }

    private void handle(@Nullable final Boolean dependenciesStrictMode) {
        final PlatformConfig platformConfig = PlatformConfig.defaultConfig();
        final ModuleConfig moduleConfigAcquisitionServer = platformConfig.getOptionalModuleConfig("acquisition-server").orElseThrow(() -> new PowsyblException("Module acquisition-server not found !!"));
        final ModuleConfig moduleConfigCaseServer = platformConfig.getOptionalModuleConfig("case-server").orElseThrow(() -> new PowsyblException("Module case-server not found !!"));
        final ModuleConfig moduleConfigCgmesBoundaryServer = platformConfig.getOptionalModuleConfig("cgmes-boundary-server").orElseThrow(() -> new PowsyblException("Module cgmes-boundary-server not found !!"));
        handle(
            Optional.ofNullable(dependenciesStrictMode).orElseGet(() -> moduleConfigAcquisitionServer.getBooleanProperty("dependencies-strict-mode", false)),
            moduleConfigCaseServer.getStringProperty("url"),
            moduleConfigCgmesBoundaryServer.getStringProperty("url"),
            moduleConfigAcquisitionServer.getStringProperty("url"),
            moduleConfigAcquisitionServer.getStringProperty("username"),
            moduleConfigAcquisitionServer.getStringProperty("password"),
            moduleConfigAcquisitionServer.getStringProperty("cases-directory"),
            moduleConfigAcquisitionServer.getStringProperty("label")
        );
    }

    public void handle(final boolean dependenciesStrictMode, final String caseServerUrl, final String cgmesBoundaryServerUrl,
                       final String acquisitionServerUsername, final String acquisitionServerPassword, final String acquisitionServerUrl,
                       final String casesDirectory, final String acquisitionServerLabel) {

        final CaseImportServiceRequester caseImportServiceRequester = new CaseImportServiceRequester(caseServerUrl);
        final CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester(cgmesBoundaryServerUrl);

        try (final AcquisitionServer acquisitionServer = new AcquisitionServer(acquisitionServerUrl, acquisitionServerUsername, acquisitionServerPassword);
             final CgmesAssemblingLogger cgmesAssemblingLogger = new CgmesAssemblingLogger(dataSource)) {
            acquisitionServer.open();

            // Get list of all tsos and business processes from cgmes boundary server
            Set<String> authorizedTsos = cgmesBoundaryServiceRequester.getTsosList();
            Set<String> authorizedBusinessProcesses = cgmesBoundaryServiceRequester.getBusinessProcessesList();

            // Get valid zip files
            Map<String, String> filesToAcquire = acquisitionServer.listFiles(casesDirectory).entrySet()
                    .stream()
                    .filter(file -> CgmesUtils.isValidProfileFileName(file.getKey(), authorizedTsos, authorizedBusinessProcesses))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
            LOGGER.info("{} valid files found on acquisition server", filesToAcquire.size());

            // Get SV files
            Map<String, String> filesSV = filesToAcquire.entrySet()
                    .stream()
                    .filter(file -> CgmesUtils.isSVFile(file.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));
            LOGGER.info("{} valid SV files found on acquisition server", filesSV.size());

            List<String> filesHandled = new ArrayList<>();
            List<String> filesAlreadyHandled = new ArrayList<>();
            List<String> filesImportingFailed = new ArrayList<>();
            List<String> filesSuccessfullyImported = new ArrayList<>();
            List<String> filesAlreadyImported = new ArrayList<>();

            for (Map.Entry<String, String> fileInfo : filesToAcquire.entrySet()) {
                if (!cgmesAssemblingLogger.isHandledFile(fileInfo.getKey(), acquisitionServerLabel)) {
                    LOGGER.info("Handling file '{}'...", fileInfo.getKey());
                    // Download the file
                    TransferableFile acquiredFile = acquisitionServer.getFile(fileInfo.getKey(), fileInfo.getValue());

                    try (ZipInputStream zipInputStream = CgmesUtils.getZipInputStream(acquiredFile.getData());
                         Reader reader = new InputStreamReader(zipInputStream)) {
                        FullModel fullModel = FullModel.parse(reader);
                        cgmesAssemblingLogger.logFileAvailable(fileInfo.getKey(), fullModel.getId(), acquisitionServerLabel, new Date());
                        cgmesAssemblingLogger.logFileDependencies(fullModel.getId(), fullModel.getDependentOn());
                    }
                    filesHandled.add(fileInfo.getKey());
                } else {
                    filesAlreadyHandled.add(fileInfo.getKey());
                }
            }

            for (Map.Entry<String, String> fileInfo : filesSV.entrySet()) {
                if (!cgmesAssemblingLogger.isImportedFile(fileInfo.getKey(), acquisitionServerLabel)) {
                    LOGGER.info("SV file '{}'...", fileInfo.getKey());
                    String uuid = cgmesAssemblingLogger.getUuidByFileName(fileInfo.getKey(), acquisitionServerLabel);

                    // Identify available and missing file dependencies
                    List<String> dependencies = CgmesUtils.getDependenciesTreeUuids(uuid, cgmesAssemblingLogger);
                    Map<String, String> availableFileDependencies = new LinkedHashMap<>();
                    Set<String> missingDependencies = new HashSet<>();
                    for (String dependUuid : dependencies) {
                        String dependFileName = cgmesAssemblingLogger.getFileNameByUuid(dependUuid, acquisitionServerLabel);
                        if (dependFileName != null) {
                            availableFileDependencies.put(dependFileName, filesToAcquire.get(dependFileName));
                        } else {
                            missingDependencies.add(dependUuid);
                        }
                    }

                    // Assembling profiles
                    TransferableFile assembledFile = CgmesUtils.prepareFinalZip(fileInfo.getKey(), availableFileDependencies,
                        missingDependencies, acquisitionServer, cgmesBoundaryServiceRequester,
                        dependenciesStrictMode, authorizedTsos, authorizedBusinessProcesses);

                    if (assembledFile != null) {
                        // Import assembled file in the case server
                        boolean importOk = caseImportServiceRequester.importCase(assembledFile);
                        if (!importOk) {
                            filesImportingFailed.add(fileInfo.getKey());
                        } else {
                            filesSuccessfullyImported.add(fileInfo.getKey());
                            cgmesAssemblingLogger.logFileImported(fileInfo.getKey(), acquisitionServerLabel, new Date());
                        }
                    } else {
                        LOGGER.error("{} file's dependencies are not resolved yet", fileInfo.getKey());
                    }
                } else {
                    filesAlreadyImported.add(fileInfo.getKey());
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
        } catch (InterruptedException e) {
            LOGGER.error("Interruption during assembling");
            Thread.currentThread().interrupt();
        } catch (Exception exc) {
            LOGGER.error("Job execution error", exc);
        }
    }
}
