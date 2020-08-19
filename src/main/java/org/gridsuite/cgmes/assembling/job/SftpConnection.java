/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.xfer.InMemoryDestFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Nicolas Noir <nicolas.noir at rte-france.com>
 */
public class SftpConnection implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SftpConnection.class);

    private final SSHClient sshClient = new SSHClient();

    private SFTPClient sftpClient = null;

    private Session session = null;

    public void open(String hostname, int port, String userName, String password) throws IOException {

        LOGGER.info("Connect to SFTP '{}' with user: '{}'", hostname, userName);

        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        sshClient.connect(hostname, port);
        sshClient.authPassword(userName, password);
        session = sshClient.startSession();
        sftpClient = sshClient.newSFTPClient();
    }

    private static class AcquiredInMemoryDestFile  extends InMemoryDestFile {
        private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        @Override
        public ByteArrayOutputStream getOutputStream() throws IOException {
            return this.outputStream;
        }
    }

    public List<Path> listFiles(String acquisitionPath) throws IOException {
        List<RemoteResourceInfo> files = sftpClient.ls(acquisitionPath);
        List<Path> filesToAcquire = new ArrayList<>();
        for (RemoteResourceInfo info : files) {
            if (info.isRegularFile()) {
                filesToAcquire.add(Path.of(info.getPath()));
            }
        }
        return filesToAcquire;
    }

    public TransferableFile getFile(String fileName) throws IOException {
        AcquiredInMemoryDestFile acquiredFile = new AcquiredInMemoryDestFile();
        sftpClient.get(fileName, acquiredFile);
        return new TransferableFile(Path.of(fileName).getFileName().toString(), acquiredFile.getOutputStream().toByteArray());
    }

    public void close() throws IOException {

        if (sftpClient != null) {
            sftpClient.close();
        }
        if (session != null) {
            session.close();
        }

        sshClient.disconnect();
    }
}
