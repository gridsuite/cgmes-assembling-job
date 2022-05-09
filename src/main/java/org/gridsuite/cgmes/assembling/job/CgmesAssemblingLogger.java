/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.gridsuite.cgmes.assembling.job.JdbcQueries.*;

/**
 * @author Chamseddine Benhamed <chamseddine.benhamed at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class CgmesAssemblingLogger implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesAssemblingLogger.class);

    public static final String FILENAME_COLUMN = "FILENAME";
    public static final String UUID_COLUMN = "UUID";
    public static final String DEPENDENCIES_COLUMN = "dependency_uuid";

    private Connection connection;

    public CgmesAssemblingLogger(DataSource dataSource) {
        try {
            this.connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isHandledFile(String filename, String origin) {
        return checkValue(SELECT_HANDLED_FILE, filename, origin);
    }

    public boolean isImportedFile(String filename, String origin) {
        return checkValue(SELECT_IMPORTED_FILE, filename, origin);
    }

    public List<String> getDependencies(String uuid) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_DEPENDENCIES)) {
            preparedStatement.setString(1, uuid);
            ResultSet resultSet = preparedStatement.executeQuery();

            List<String> result = new ArrayList<>();

            while (resultSet.next()) {
                result.add(resultSet.getString(DEPENDENCIES_COLUMN));
            }

            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void logFileAvailable(String fileName, String uuid, String origin, Date date) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_HANDLED_FILE)) {
            preparedStatement.setString(1, fileName);
            preparedStatement.setString(2, origin);
            preparedStatement.setDate(3, new java.sql.Date(date.getTime()));
            preparedStatement.setString(4, uuid);
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void logFileImported(String fileName, String origin, Date date) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_IMPORTED_FILE)) {
            preparedStatement.setString(1, fileName);
            preparedStatement.setString(2, origin);
            preparedStatement.setDate(3, new java.sql.Date(date.getTime()));
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void logFileDependencies(String uuid, List<String> dependencies) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_DEPENDENCIES)) {
            for (String dependency : dependencies) {
                if (dependency != null) {
                    preparedStatement.setString(1, uuid);
                    preparedStatement.setString(2, dependency);
                    preparedStatement.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        LOGGER.info("Add dependency between file {} and files {}", uuid, dependencies);
    }

    public String getFileNameByUuid(String uuid, String origin) {
        return getValue(uuid, origin, SELECT_FILENAME_BY_UUID, FILENAME_COLUMN);
    }

    public String getUuidByFileName(String filename, String origin) {
        return getValue(filename, origin, SELECT_UUID_BY_FILENAME, UUID_COLUMN);
    }

    private String getValue(String file, String origin, String query, String columnName) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, file);
            preparedStatement.setString(2, origin);
            ResultSet resultSet = preparedStatement.executeQuery();

            String result = null;
            if (resultSet.next()) {
                result = resultSet.getString(columnName);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkValue(String query, String filename, String origin) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setString(1, filename);
            preparedStatement.setString(2, origin);
            ResultSet resultSet = preparedStatement.executeQuery();

            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            LOGGER.error("Error closing connection", e);
        }
    }

    public Connection getConnection() {
        return connection;
    }
}
