/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Nicolas Noir <nicolas.noir at rte-france.com>
 */
public class AcquisitionServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AcquisitionServer.class);

    private final StandardFileSystemManager fsManager = new StandardFileSystemManager();
    private FileSystemOptions fsOptions = new FileSystemOptions();

    private String serverUrl;

    public AcquisitionServer(String url, String userName, String password) throws FileSystemException {
        serverUrl = url;

        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, userName, password);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(fsOptions, auth);

        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(fsOptions, true);
        SftpFileSystemConfigBuilder.getInstance().setPreferredAuthentications(fsOptions, "publickey,password");
        SftpFileSystemConfigBuilder.getInstance().setConnectTimeoutMillis(fsOptions, 30000);

        FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(fsOptions, true);
        FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(fsOptions, 30000);
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(fsOptions, true);
    }

    public void open() throws FileSystemException {
        fsManager.init();
    }

    private static class FileObjectComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject fo1, FileObject fo2) {
            if (fo1 == fo2) {
                return 0;
            } else {
                try (final FileContent fo1Content = fo1.getContent();
                     final FileContent fo2Content = fo2.getContent()) {
                    return Long.compare(fo1Content.getLastModifiedTime(), fo2Content.getLastModifiedTime());
                } catch (FileSystemException e) {
                    // if error when getting content, order by name
                    return fo1.compareTo(fo2);
                }
            }
        }
    }

    public Map<String, String> listFiles(String acquisitionDirPath) throws IOException {
        FileObject serverRoot = fsManager.resolveFile(serverUrl, fsOptions);
        FileObject acquisitionDirectory = serverRoot.resolveFile(acquisitionDirPath);
        // sorting files by modification time in reverse order (most recent files first)
        List<FileObject> childrenFiles = Arrays.stream(acquisitionDirectory.getChildren()).filter(f -> {
            try {
                return f.isFile();
            } catch (FileSystemException e) {
                return false;
            }
        }).sorted(new FileObjectComparator().reversed()).collect(Collectors.toList());

        Map<String, String> childrenUrls = new LinkedHashMap<>();
        for (FileObject child : childrenFiles) {
            try {
                String childName = child.getName().getBaseName();
                String childUrl = child.getURL().toString();
                childrenUrls.put(childName, childUrl);
            } catch (FileSystemException e) {
                LOGGER.warn(e.getMessage());
            }
        }

        return childrenUrls;
    }

    public TransferableFile getFile(String fileName, String fileUrl) throws IOException {
        FileObject file = fsManager.resolveFile(fileUrl, fsOptions);
        return new TransferableFile(fileName, file.getContent().getByteArray());
    }

    @Override
    public void close() throws IOException {
        fsManager.close();
    }
}
