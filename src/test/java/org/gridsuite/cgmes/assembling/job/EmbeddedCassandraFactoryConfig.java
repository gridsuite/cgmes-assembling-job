/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory;
import com.github.nosan.embedded.cassandra.api.CassandraFactory;
import com.github.nosan.embedded.cassandra.api.Version;
import com.github.nosan.embedded.cassandra.artifact.RemoteArtifact;
import com.github.nosan.embedded.cassandra.commons.io.ClassPathResource;

import java.net.*;
import java.time.Duration;

/**
 * @author Nicolas Noir <nicolas.noir at rte-france.com>
 */
public final class EmbeddedCassandraFactoryConfig {

    private EmbeddedCassandraFactoryConfig() {
    }

    static CassandraFactory embeddedCassandraFactory() {
        EmbeddedCassandraFactory cassandraFactory = new EmbeddedCassandraFactory();
        RemoteArtifact artifact = new RemoteArtifact(Version.of("4.0-alpha4"));
        String proxyHost = System.getProperty("https.proxyHost", System.getProperty("http.proxyHost", System.getProperty("proxyHost")));
        if (proxyHost != null && !proxyHost.isEmpty()) {
            String proxyPort = System.getProperty("https.proxyPort", System.getProperty("http.proxyPort", System.getProperty("proxyPort")));
            String proxyUser = System.getProperty("https.proxyUser", System.getProperty("http.proxyUser", System.getProperty("proxyUser")));
            if (proxyUser != null && !proxyUser.isEmpty()) {
                String proxyPassword = System.getProperty("https.proxyPassword", System.getProperty("http.proxyPassword", System.getProperty("proxyPassword")));
                Authenticator.setDefault(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        if (getRequestorType() == RequestorType.PROXY) {
                            String prot = getRequestingProtocol().toLowerCase();
                            String host = System.getProperty(prot + ".proxyHost", proxyHost);
                            String port = System.getProperty(prot + ".proxyPort", proxyPort);
                            String user = System.getProperty(prot + ".proxyUser", proxyUser);
                            String password = System.getProperty(prot + ".proxyPassword", proxyPassword);
                            if (getRequestingHost().equalsIgnoreCase(host)) {
                                if (Integer.parseInt(port) == getRequestingPort()) {
                                    return new PasswordAuthentication(user, password.toCharArray());
                                }
                            }
                        }
                        return null;
                    }
                });
            }
            artifact.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort))));
        }
        cassandraFactory.setArtifact(artifact);
        cassandraFactory.setConfig(new ClassPathResource("cassandra.yaml"));
        cassandraFactory.setPort(9142);
        cassandraFactory.setJmxLocalPort(0);
        cassandraFactory.setRpcPort(0);
        cassandraFactory.setStoragePort(16432);
        try {
            cassandraFactory.setAddress(InetAddress.getByName("localhost"));
        } catch (UnknownHostException e) {
            cassandraFactory.setAddress(null);
        }
        cassandraFactory.setTimeout(Duration.ofSeconds(900)); //default is 90, we are getting timeouts on GH actions
        return cassandraFactory;
    }
}
