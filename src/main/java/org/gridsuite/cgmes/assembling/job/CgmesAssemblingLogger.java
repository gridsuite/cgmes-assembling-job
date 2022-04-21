/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.cgmes.assembling.job;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private JdbcConnector connector;

    public static final String DB_CHANGELOG_MASTER = "db/changelog/db.changelog-master.yaml";

    public void connectDb(String url, String username, String password) {
        connector = new JdbcConnector(url, username, password);
        try {
            // liquibase creates the connection and closes it
            // (normal because it could use a separate user, or set special flags on the connection)
            updateLiquibase(connector);
        } catch (DatabaseException e) {
            throw new RuntimeException(e);
        }

        // Create another connection for regular operations
        connector.connect();
    }

    private void updateLiquibase(JdbcConnector connector) throws DatabaseException {
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connector.connect()));
        Properties properties = new Properties();
        try (Liquibase liquibase = new Liquibase(DB_CHANGELOG_MASTER,
                new ClassLoaderResourceAccessor(),
                database);) {
            properties.forEach((key, value) -> liquibase.setChangeLogParameter(Objects.toString(key), value));
            liquibase.update(new Contexts(), new LabelExpression());
        } catch (Exception e) {
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
        try (PreparedStatement preparedStatement = connector.getConnection().prepareStatement(SELECT_DEPENDENCIES)) {
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
        try (PreparedStatement preparedStatement = connector.getConnection().prepareStatement(INSERT_HANDLED_FILE)) {
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
        try (PreparedStatement preparedStatement = connector.getConnection().prepareStatement(INSERT_IMPORTED_FILE)) {
            preparedStatement.setString(1, fileName);
            preparedStatement.setString(2, origin);
            preparedStatement.setDate(3, new java.sql.Date(date.getTime()));
            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void logFileDependencies(String uuid, List<String> dependencies) {
        try (PreparedStatement preparedStatement = connector.getConnection().prepareStatement(INSERT_DEPENDENCIES)) {
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
        try (PreparedStatement preparedStatement = connector.getConnection().prepareStatement(query)) {
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
        try (PreparedStatement preparedStatement = connector.getConnection().prepareStatement(query)) {
            preparedStatement.setString(1, filename);
            preparedStatement.setString(2, origin);
            ResultSet resultSet = preparedStatement.executeQuery();

            return resultSet.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (connector != null) {
            connector.close();
        }
    }

    public JdbcConnector getConnector() {
        return connector;
    }
}
