/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * @author Nicolas Noir <nicolas.noir at rte-france.com>
 */
public class CassandraConnector {
    private Cluster cluster;

    private Session session;

    public void connect(final String node, final int port) {
        this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        cluster.close();
    }
}
