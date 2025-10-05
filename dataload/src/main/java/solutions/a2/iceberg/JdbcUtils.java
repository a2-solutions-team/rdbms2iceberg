/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package solutions.a2.iceberg;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.sql.Types.DATE;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.BINARY;
import static java.sql.Types.CHAR;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.ROWID;
import static java.sql.Types.CLOB;
import static java.sql.Types.NCLOB;
import static java.sql.Types.BLOB;
import static java.sql.Types.SQLXML;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 *
 */
public class JdbcUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtils.class);

    static final int DB_TYPE_ORACLE = 1;
    static final int DB_TYPE_SQLSERVER = 2;
    static final int DB_TYPE_DB2 = 3;
    static final int DB_TYPE_POSTGRESQL = 4;
    static final int DB_TYPE_MYSQL = 5;
    static final int DB_TYPE_SQLITE = 6;

    private static final String DRIVER_ORACLE = "oracle.jdbc.OracleDriver";
    private static final String PREFIX_ORACLE = "jdbc:oracle:";
    private static final String DRIVER_SQLSERVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    private static final String PREFIX_SQLSERVER = "jdbc:sqlserver:";
    private static final String DRIVER_DB2 = "com.ibm.db2.jcc.DB2Driver";
    private static final String PREFIX_DB2 = "jdbc:db2:";
    private static final String DRIVER_POSTGRESQL = "org.postgresql.Driver";
    private static final String PREFIX_POSTGRESQL = "jdbc:postgresql:";
    private static final String DRIVER_MARIADB = "org.mariadb.jdbc.Driver";
    private static final String PREFIX_MYSQL = "jdbc:mysql:";
    private static final String PREFIX_MARIADB = "jdbc:mariadb:";
    private static final String DRIVER_SQLITE = "org.sqlite.JDBC";
    private static final String PREFIX_SQLITE = "jdbc:sqlite:";

    static String getTypeName(final int jdbcType) {
        switch (jdbcType) {
            case DATE -> {
                return "DATE";
            }
            case TIMESTAMP -> {
                return "TIMESTAMP";
            }
            case TIMESTAMP_WITH_TIMEZONE -> {
                return "TIMESTAMP_WITH_TIMEZONE";
            }
            case BOOLEAN -> {
                return "BOOLEAN";
            }
            case TINYINT -> {
                return "TINYINT";
            }
            case SMALLINT -> {
                return "SMALLINT";
            }
            case INTEGER -> {
                return "INTEGER";
            }
            case BIGINT -> {
                return "BIGINT";
            }
            case FLOAT -> {
                return "FLOAT";
            }
            case DOUBLE -> {
                return "DOUBLE";
            }
            case DECIMAL -> {
                return "DECIMAL";
            }
            case NUMERIC -> {
                return "NUMERIC";
            }
            case BINARY -> {
                return "BINARY";
            }
            case CHAR -> {
                return "CHAR";
            }
            case VARCHAR -> {
                return "VARCHAR";
            }
            case NCHAR -> {
                return "NCHAR";
            }
            case NVARCHAR -> {
                return "NVARCHAR";
            }
            case ROWID -> {
                return "ROWID";
            }
            case CLOB -> {
                return "CLOB";
            }
            case NCLOB -> {
                return "NCLOB";
            }
            case BLOB -> {
                return "BLOB";
            }
            case SQLXML -> {
                return "XMLTYPE";
            }
        }
        return "UNSUPPORTED!!!";
    }

    static int getTypeId(final String jdbcTypeName) {
        switch (jdbcTypeName) {
            case "DATE" -> {
                return DATE;
            }
            case "TIMESTAMP" -> {
                return TIMESTAMP;
            }
            case "TIMESTAMP_WITH_TIMEZONE" -> {
                return TIMESTAMP_WITH_TIMEZONE;
            }
            case "BOOLEAN" -> {
                return BOOLEAN;
            }
            case "TINYINT" -> {
                return TINYINT;
            }
            case "SMALLINT" -> {
                return SMALLINT;
            }
            case "INTEGER" -> {
                return INTEGER;
            }
            case "BIGINT" -> {
                return BIGINT;
            }
            case "FLOAT" -> {
                return FLOAT;
            }
            case "DOUBLE" -> {
                return DOUBLE;
            }
            case "DECIMAL" -> {
                return DECIMAL;
            }
            case "NUMERIC" -> {
                return NUMERIC;
            }
            case "BINARY" -> {
                return BINARY;
            }
            case "CHAR" -> {
                return CHAR;
            }
            case "VARCHAR" -> {
                return VARCHAR;
            }
            case "NCHAR" -> {
                return NCHAR;
            }
            case "NVARCHAR" -> {
                return NVARCHAR;
            }
            case "ROWID" -> {
                return ROWID;
            }
            case "CLOB" -> {
                return CLOB;
            }
            case "NCLOB" -> {
                return NCLOB;
            }
            case "BLOB" -> {
                return BLOB;
            }
            case "XMLTYPE" -> {
                return SQLXML;
            }
        }
        return Integer.MIN_VALUE;
    }

    static boolean isNumeric(final int jdbcType) {
        return switch (jdbcType) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, NUMERIC ->
                true;
            default ->
                false;
        };
    }

    static final String[] NUMERICS = {
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC"
    };

    static int loadJdbcDriver(final String jdbcUrl) {
        if (Strings.CS.startsWith(jdbcUrl, PREFIX_ORACLE)) {
            if (!isDriverLoaded(DRIVER_ORACLE)) {
                try {
                     Class.forName(DRIVER_ORACLE);
                } catch (ClassNotFoundException cnf) {}
            }
            return DB_TYPE_ORACLE;
        } else if (Strings.CS.startsWith(jdbcUrl, PREFIX_SQLSERVER)) {
            if (!isDriverLoaded(DRIVER_SQLSERVER)) {
                try {
                     Class.forName(DRIVER_SQLSERVER);
                } catch (ClassNotFoundException cnf) {}
            }
            return DB_TYPE_SQLSERVER;
        } else if (Strings.CS.startsWith(jdbcUrl, PREFIX_DB2)) {
            if (!isDriverLoaded(DRIVER_DB2)) {
                try {
                     Class.forName(DRIVER_DB2);
                } catch (ClassNotFoundException cnf) {}
            }
            return DB_TYPE_DB2;
        } else if (Strings.CS.startsWith(jdbcUrl, PREFIX_POSTGRESQL)) {
            if (!isDriverLoaded(DRIVER_POSTGRESQL)) {
                try {
                     Class.forName(DRIVER_POSTGRESQL);
                } catch (ClassNotFoundException cnf) {}
            }
            return DB_TYPE_POSTGRESQL;
        } else if (Strings.CS.startsWith(jdbcUrl, PREFIX_MYSQL) ||
                    Strings.CS.startsWith(jdbcUrl, PREFIX_MARIADB)) {
            if (!isDriverLoaded(DRIVER_MARIADB)) {
                try {
                     Class.forName(DRIVER_MARIADB);
                } catch (ClassNotFoundException cnf) {}
            }
            return DB_TYPE_MYSQL;
        } else if (Strings.CS.startsWith(jdbcUrl, PREFIX_SQLITE)) {
            if (!isDriverLoaded(DRIVER_SQLITE)) {
                try {
                     Class.forName(DRIVER_SQLITE);
                } catch (ClassNotFoundException cnf) {}
            }
            return DB_TYPE_SQLITE;
        } else {
            LOGGER.error(
                    """
                    
                    =====================
                    Unable to onnect to JDBC URL '{}'!
                    =====================
                    """,
                        jdbcUrl);
            throw new UnsupportedOperationException("Unable to connect to JDBC URL '" + jdbcUrl + "'!");
        }
        

    }

    private static boolean isDriverLoaded(final String driverClass) {
        final Enumeration<Driver> availableDrivers = DriverManager.getDrivers();
        while (availableDrivers.hasMoreElements()) {
            final Driver driver = availableDrivers.nextElement();
            if (Strings.CS.equals(driverClass, driver.getClass().getCanonicalName())) {
                return true;
            }
        }
        return false;
    }

}
