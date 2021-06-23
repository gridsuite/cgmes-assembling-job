/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;

/**
 * @author Nicolas Noir <nicolas.noir at rte-france.com>
 */
public class CassandraConnector {

    private CqlSession session;

    public void connect(final String node, final int port, String datacenter) {
        var inetAddress = new InetSocketAddress(node, port);
        this.session = CqlSession.builder().addContactPoint(inetAddress).withLocalDatacenter(datacenter).build();
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }
}
