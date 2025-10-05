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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static solutions.a2.iceberg.JdbcUtils.loadJdbcDriver;
import static solutions.a2.iceberg.JdbcUtils.DB_TYPE_ORACLE;
import static solutions.a2.iceberg.JdbcUtils.DB_TYPE_SQLSERVER;
import static solutions.a2.iceberg.JdbcUtils.DB_TYPE_POSTGRESQL;

/**
 *
 * Ora2Iceberg entry point
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class DataLoad {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataLoad.class);
    private static final Pattern SQL_EXPRESSION = Pattern.compile(
            "(.*?)SELECT(.*?)FROM(.*?)",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
    private static final long MAX_FILE_SIZE = 0x08000000;
    private static final String DEFAULT_NUMBER_FORMAT = "decimal(38,10)";

    static final String PARTITION_TYPE_IDENTITY = "IDENTITY";
    static final String PARTITION_TYPE_BUCKET = "BUCKET";
    static final String PARTITION_TYPE_TRUNCATE = "TRUNCATE";
    static final String PARTITION_TYPE_YEAR = "YEAR";
    static final String PARTITION_TYPE_MONTH = "MONTH";
    static final String PARTITION_TYPE_DAY = "DAY";
    static final String PARTITION_TYPE_HOUR = "HOUR";

    static final String ROWID_KEY = "ORA_ROW_ID";
    static final String ROWID_ORA = "ROWID";

    static final String UPLOAD_DEFAULT_MODE = "full";

    private static final String CATALOG_IMPL_REST = "REST";
    private static final String CATALOG_IMPL_JDBC = "JDBC";
    private static final String CATALOG_IMPL_HADOOP = "HADOOP";
    private static final String CATALOG_IMPL_HIVE = "HIVE";
    private static final String CATALOG_IMPL_NESSIE = "NESSIE";
    private static final String CATALOG_IMPL_GLUE = "GLUE";
    private static final String CATALOG_IMPL_S3TABLES = "S3TABLES";
    private static final String CATALOG_IMPL_DYNAMODB = "DYNAMODB";
    private static final String CATALOG_IMPL_SNOWFLAKE = "SNOWFLAKE";
    private static final Map<String, String> CATALOG_IMPL = new HashMap<>();

    static {
        CATALOG_IMPL.put(CATALOG_IMPL_REST, "org.apache.iceberg.rest.RESTCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_JDBC, "org.apache.iceberg.jdbc.JdbcCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_HADOOP, "org.apache.iceberg.hadoop.HadoopCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_HIVE, "org.apache.iceberg.hive.HiveCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_NESSIE, "org.apache.iceberg.nessie.NessieCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_GLUE, "org.apache.iceberg.aws.glue.GlueCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_S3TABLES, "software.amazon.s3tables.iceberg.S3TablesCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_DYNAMODB, "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog");
        CATALOG_IMPL.put(CATALOG_IMPL_SNOWFLAKE, "org.apache.iceberg.snowflake.SnowflakeCatalog");
    }

    private static final String OPT_ICEBERG_PARTITION = "iceberg-partition";
    private static final String OPT_ICEBERG_PARTITION_SHORT = "P";
    private static final String OPT_ICEBERG_CATALOG_IMPL = "iceberg-catalog-type";
    private static final String OPT_ICEBERG_CATALOG_IMPL_SHORT = "T";
    private static final String OPT_ICEBERG_CATALOG_URI = "iceberg-catalog-uri";
    private static final String OPT_ICEBERG_CATALOG_URI_SHORT = "U";
    private static final String OPT_ICEBERG_SOURCE_SCHEMA = "source-schema";
    private static final String OPT_ICEBERG_SOURCE_SCHEMA_SHORT = "s";
    private static final String OPT_ICEBERG_SOURCE_OBJECT = "source-object";
    private static final String OPT_ICEBERG_SOURCE_OBJECT_SHORT = "o";
    private static final String OPT_ICEBERG_NAMESPACE = "iceberg-namespace";
    private static final String OPT_ICEBERG_NAMESPACE_SHORT = "N";
    private static final String OPT_ICEBERG_TABLE = "iceberg-table";
    private static final String OPT_ICEBERG_TABLE_SHORT = "t";
    private static final String OPT_ICEBERG_PROPS = "iceberg-catalog-properties";
    private static final String OPT_ICEBERG_PROPS_SHORT = "R";
    private static final String OPT_SOURCE_JDBC_URL = "source-jdbc-url";
    private static final String OPT_SOURCE_JDBC_URL_SHORT = "j";
    private static final String OPT_SOURCE_JDBC_USER = "source-user";
    private static final String OPT_SOURCE_JDBC_USER_SHORT = "u";
    private static final String OPT_SOURCE_JDBC_PW = "source-password";
    private static final String OPT_SOURCE_JDBC_PW_SHORT = "p";
    private static final String OPT_WHERE_CLAUSE = "where-clause";
    private static final String OPT_WHERE_CLAUSE_SHORT = "w";
    private static final String OPT_DATA_TYPE_MAP = "data-type-map";
    private static final String OPT_DATA_TYPE_MAP_SHORT = "m";
    private static final String OPT_ICEBERG_ID_COLS = "iceberg-id-columns";
    private static final String OPT_ICEBERG_ID_COLS_SHORT = "I";
    private static final String OPT_ICEBERG_MAX_SIZE = "iceberg-max-file-size";
    private static final String OPT_ICEBERG_MAX_SIZE_SHORT = "Z";

    @SuppressWarnings("unchecked")
    public static void main(String[] argv) {
        LOGGER.info("Starting...");

        // Command line options
        final Options options = new Options();
        setupCliOptions(options);

        final CommandLineParser parser = new DefaultParser();
        final HelpFormatter formatter = HelpFormatter.builder()
                .get();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, argv);
        } catch (ParseException pe) {
            LOGGER.error(pe.getMessage());
            try {
                formatter.printHelp(DataLoad.class.getCanonicalName(), "", options, "", true);
            } catch (IOException ioe) {
            }
            System.exit(1);
        }

        final Map<String, String> catalogProps = new HashMap<>();
        if (!Strings.CI.equals(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT), CATALOG_IMPL_GLUE)
                && !Strings.CI.equals(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT), CATALOG_IMPL_S3TABLES)) {
            final String icebergCatalogUri = cmd.getOptionValue(OPT_ICEBERG_CATALOG_URI_SHORT);
            if (StringUtils.isBlank(icebergCatalogUri)) {
                LOGGER.error("""
                             
                             =====================
                             The --{}/-{} parameter cannot be empty when using the 
                             catalog type --{}/-{} set to {}). Please provide a valid URI for the catalog server
                             =====================
                             """,
                        OPT_ICEBERG_CATALOG_URI, OPT_ICEBERG_CATALOG_URI_SHORT,
                        OPT_ICEBERG_CATALOG_IMPL, OPT_ICEBERG_CATALOG_IMPL_SHORT,
                        StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT)));
                System.exit(1);
            }
            catalogProps.put(CatalogProperties.URI, icebergCatalogUri);
        }
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, cmd.getOptionValue("iceberg-warehouse"));
        switch (StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT))) {
            case CATALOG_IMPL_REST, CATALOG_IMPL_JDBC, CATALOG_IMPL_HADOOP, CATALOG_IMPL_HIVE, CATALOG_IMPL_NESSIE, CATALOG_IMPL_GLUE, CATALOG_IMPL_S3TABLES, CATALOG_IMPL_DYNAMODB, CATALOG_IMPL_SNOWFLAKE ->
                catalogProps.put(CatalogProperties.CATALOG_IMPL,
                        CATALOG_IMPL.get(StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT))));
            default -> {
                try {
                    final Class<?> clazz = Class.forName(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT));
                    if (!clazz.isAssignableFrom(BaseMetastoreCatalog.class)) {
                        LOGGER.error("""
                                     
                                     =====================
                                     Class {} must extend {}!
                                     =====================
                                     """,
                                clazz.getCanonicalName(),
                                BaseMetastoreCatalog.class.getCanonicalName());
                        System.exit(1);
                    }
                    catalogProps.put(CatalogProperties.CATALOG_IMPL,
                            cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT));
                } catch (ClassNotFoundException cnfe) {
                    LOGGER.error("""
                                 
                                 =====================
                                 Unable to load class {} specified as an Apache Iceberg catalog implementation!
                                 The following exception occured:
                                 {}
                                 
                                 =====================
                                 """,
                            cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT), cnfe.getMessage());
                    System.exit(1);
                }
            }
        }
        final String[] params = cmd.getOptionValues(OPT_ICEBERG_PROPS_SHORT);
        if (params != null && params.length > 0) {
            if (params.length % 2 == 0) {
                for (int i = 0; i < params.length; i += 2) {
                    catalogProps.put(params[i], params[i + 1]);
                }
            } else {
                LOGGER.error("""
                             
                             =====================
                             Unable to parse from command line values of Apache Iceberg Catalog properties!
                             Please check parameters!
                             =====================
                             """);
                System.exit(1);
            }
        }
        if (Strings.CS.equals(CATALOG_IMPL_JDBC, StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT))))
            loadJdbcDriver(catalogProps.get(CatalogProperties.URI));

        Catalog catalog = null;
        try {
            final Class<?> clazz = Class.forName(catalogProps.get(CatalogProperties.CATALOG_IMPL));
            final Constructor<?> constructor = clazz.getConstructor();
            catalog = (Catalog) constructor.newInstance();
            if (catalog instanceof Configurable) {
                //EcsCatalog, GlueCatalog, JdbcCatalog, NessieCatalog, RESTCatalog, RESTSessionCatalog, SnowflakeCatalog
                ((Configurable<Object>) catalog).setConf(new Configuration());
            }
            catalog.initialize(cmd.getOptionValue("iceberg-catalog"), catalogProps);
        } catch (ClassNotFoundException cnfe) {
            LOGGER.error("""
                         
                         =====================
                         Unable to load class {} specified as an Apache Iceberg catalog implementation!
                         The following exception occured:
                         {}
                         
                         =====================
                         """,
                    catalogProps.get(CatalogProperties.CATALOG_IMPL), cnfe.getMessage());
            System.exit(1);
        } catch (NoSuchMethodException | SecurityException ce) {
            LOGGER.error("""
                         
                         =====================
                         Unable to find no-arg constructor for class {} specified as an Apache Iceberg catalog implementation!
                         The following exception occured:
                         {}
                         {}
                         =====================
                         """,
                    catalogProps.get(CatalogProperties.CATALOG_IMPL), ce.getMessage(), ExceptionUtils.getStackTrace(ce));
            System.exit(1);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ie) {
            LOGGER.error("""
                         
                         =====================
                         Unable to instantiate constructor for class {} specified as an Apache Iceberg catalog implementation!
                         The following exception occured:
                         {}
                         {}
                         =====================
                         """,
                    catalogProps.get(CatalogProperties.CATALOG_IMPL), ie.getMessage(), ExceptionUtils.getStackTrace(ie));
            System.exit(1);
        }
        LOGGER.info("""
                    
                    =====================
                    Connected to Apache Iceberg Catalog {} located on {}
                    =====================
                    """,
                catalog.name(), catalogProps.get(CatalogProperties.URI));

        final String sourceUrl = cmd.getOptionValue(OPT_SOURCE_JDBC_URL_SHORT);
        final int dbType = loadJdbcDriver(sourceUrl);
        final String sourceUser = cmd.getOptionValue(OPT_SOURCE_JDBC_USER_SHORT);
        final String sourcePassword = cmd.getOptionValue(OPT_SOURCE_JDBC_PW_SHORT);
        final String whereClause = cmd.getOptionValue(OPT_WHERE_CLAUSE_SHORT);
        final String dataTypeMap = cmd.getOptionValue(OPT_DATA_TYPE_MAP_SHORT);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(sourceUrl, sourceUser, sourcePassword);
        } catch (SQLException sqle) {
            LOGGER.error("""
                         
                         =====================
                         Unable to connect to RDBMS using jdbcUrl '{}' as user '{}' with password '{}'!
                         Exception: {}{}
                         =====================
                         """,
                    sourceUrl, sourceUser, sourcePassword, sqle.getMessage(), ExceptionUtils.getStackTrace(sqle));
            System.exit(1);
        }

        try {
            final DatabaseMetaData dbMetaData = connection.getMetaData();
            LOGGER.info("""
                        
                        =====================
                        Connected to {}{}
                        using {} {}
                        =====================
                        """,
                    dbMetaData.getDatabaseProductName(), dbMetaData.getDatabaseProductVersion(),
                    dbMetaData.getDriverName(), dbMetaData.getDriverVersion());
            final String sourceSchema;
            if (StringUtils.isBlank(cmd.getOptionValue(OPT_ICEBERG_SOURCE_SCHEMA_SHORT))) {
                switch (dbType) {
                    case DB_TYPE_ORACLE -> sourceSchema = dbMetaData.getUserName();
                    case DB_TYPE_SQLSERVER -> sourceSchema = "dbo";
                    case DB_TYPE_POSTGRESQL -> sourceSchema = "public";
                    default -> sourceSchema = null;
                }
            } else {
                if (dbType == DB_TYPE_ORACLE) {
                    if (Strings.CS.startsWith(cmd.getOptionValue(OPT_ICEBERG_SOURCE_SCHEMA_SHORT), "\"")
                        && Strings.CS.endsWith(cmd.getOptionValue(OPT_ICEBERG_SOURCE_SCHEMA_SHORT), "\"")) {
                        sourceSchema = cmd.getOptionValue(OPT_ICEBERG_SOURCE_SCHEMA_SHORT);
                    } else {
                        sourceSchema = StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_SOURCE_SCHEMA_SHORT));
                    }
                } else {
                    sourceSchema = cmd.getOptionValue(OPT_ICEBERG_SOURCE_SCHEMA_SHORT);
                }
            }

            final String sourceObject;
            final boolean isTableOrView;
            if (StringUtils.containsWhitespace(cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT))) {
                isTableOrView = false;
                if (SQL_EXPRESSION.matcher(cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT)).matches()) {
                    sourceObject = cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT);
                } else {
                    sourceObject = null;
                    LOGGER.error("""
                                 
                                 =====================
                                 '{}' is not a valid SQL SELECT statement!
                                 =====================
                                 """,
                            cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT));
                    System.exit(1);
                }
            } else {
                isTableOrView = true;
                if (Strings.CS.startsWith(cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT), "\"")
                        && Strings.CS.endsWith(cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT), "\"")) {
                    sourceObject = StringUtils.substringBetween(cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT), "");
                } else {
                    sourceObject = StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_SOURCE_OBJECT_SHORT));
                }
            }

            if (cmd.hasOption(OPT_WHERE_CLAUSE_SHORT)) {
                if (!isTableOrView) {
                    LOGGER.error("""
                                 
                                 =====================
                                 WHERE clause can be provided only for a table or view!
                                 =====================
                                 """);
                    System.exit(1);
                }
                //TODO Do we need to check syntax for WHERE CLAUSE?

            }

            final String icebergTableName;
            if (StringUtils.isBlank(cmd.getOptionValue(OPT_ICEBERG_TABLE_SHORT)) && !isTableOrView) {
                icebergTableName = null;
                LOGGER.error("""
                             
                             =====================
                             Must specify destination table using {}/{} name when using SQL STATEMENT as source!
                             =====================
                             """,
                        OPT_ICEBERG_TABLE_SHORT, OPT_ICEBERG_TABLE);
                System.exit(1);
            } else {
                //Changing logic to use Default value in getOptionValue
                //TODO
                //TODO - what if is not table or view???
                //TODO
                icebergTableName = cmd.getOptionValue(OPT_ICEBERG_TABLE_SHORT, sourceObject);
            }

            final TableIdentifier icebergTable;
            switch (StringUtils.upperCase(cmd.getOptionValue(OPT_ICEBERG_CATALOG_IMPL_SHORT))) {
                case CATALOG_IMPL_GLUE -> {
                    final String glueDb = StringUtils.isBlank(cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT))
                            ? sourceSchema : cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT);
                    if ((catalogProps.containsKey(AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION)
                            && Strings.CI.equals(catalogProps.get(AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION), "false"))
                            || (!catalogProps.containsKey(AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION)
                            && !AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION_DEFAULT)) {
                        LOGGER.warn("""
                                    
                                    =====================
                                    Converting Oracle upper case SCHEMA/TABLE/COLUMN names to AWS Glue lower case names
                                    =====================
                                    """);
                        icebergTable = TableIdentifier.of(
                                StringUtils.lowerCase(glueDb), StringUtils.lowerCase(icebergTableName));
                    } else {
                        icebergTable = TableIdentifier.of(glueDb, icebergTableName);
                    }
                    try {
                        if (!AwsUtil.checkAndCreateGlueDbIfMissed(icebergTable.namespace().toString())) {
                            LOGGER.error("""
                                         
                                         =====================
                                         Unable to check or create AWS Glue database {}!
                                         =====================
                                         """,
                                    icebergTable.namespace().toString());
                            System.exit(1);
                        }
                    } catch (IOException ioe) {
                        LOGGER.error("""
                                     
                                     =====================
                                     AWS  SDK error {}!
                                     {}
                                     
                                     =====================
                                     """,
                                ioe.getMessage(), ExceptionUtils.getStackTrace(ioe));
                        System.exit(1);
                    }
                }
                case CATALOG_IMPL_S3TABLES -> {
                    final String s3TablesDb = StringUtils.isBlank(cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT))
                            ? sourceSchema : cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT);
                    LOGGER.warn("""
                                
                                =====================
                                Converting Oracle upper case SCHEMA/TABLE/COLUMN names to AWS S3 Tables lower case names
                                =====================
                                """);
                    icebergTable = TableIdentifier.of(
                            StringUtils.lowerCase(s3TablesDb), StringUtils.lowerCase(icebergTableName));
                    try {
                        if (!AwsUtil.checkAndCreateS3TablesDbIfMissed(
                                icebergTable.namespace().toString(),
                                catalogProps.get(CatalogProperties.WAREHOUSE_LOCATION))) {
                            LOGGER.error("""
                                         
                                         =====================
                                         Unable to check or create AWS S3 Tables namespace {}!
                                         =====================
                                         """,
                                    icebergTable.namespace().toString());
                            System.exit(1);
                        }
                    } catch (IOException ioe) {
                        LOGGER.error("""
                                     
                                     =====================
                                     AWS  SDK error {}!
                                     {}
                                     
                                     =====================
                                     """,
                                ioe.getMessage(), ExceptionUtils.getStackTrace(ioe));
                        System.exit(1);
                    }
                }
                case CATALOG_IMPL_NESSIE, CATALOG_IMPL_REST -> {
                    final String nessieNamespace = cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT);
                    if (StringUtils.isBlank(nessieNamespace)) // Nessie namespaces are implicit and do not need to be explicitly created or deleted.
                        // The create and delete namespace methods are no-ops for the NessieCatalog.
                    {
                        icebergTable = TableIdentifier.of(icebergTableName);
                    } else {
                        icebergTable = TableIdentifier.of(
                                Namespace.of(StringUtils.split(nessieNamespace, '.')),
                                StringUtils.lowerCase(icebergTableName));
                    }
                }
                case CATALOG_IMPL_SNOWFLAKE -> {
                    final String snowNamespace = cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT);
                    if (StringUtils.isBlank(snowNamespace)) {
                        LOGGER.error("""
                                     
                                     =====================
                                     Must specify namespace for Snowflake catalog!
                                     =====================
                                     """);
                        System.exit(1);
                    }
                    icebergTable = TableIdentifier.of(
                            Namespace.of(StringUtils.split(snowNamespace, '.')), icebergTableName);
                }
                default -> {
                    final Namespace namespace;
                    if (StringUtils.isBlank(cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT))) {
                        namespace = Namespace.of(sourceSchema);
                    } else {
                        namespace = Namespace.of(cmd.getOptionValue(OPT_ICEBERG_NAMESPACE_SHORT));
                    }
                    icebergTable = TableIdentifier.of(namespace, icebergTableName);
                }
            }

            String uploadModeValue = cmd.getOptionValue("upload-mode", UPLOAD_DEFAULT_MODE);
            boolean icebergTableExists = catalog.tableExists(icebergTable);

            switch (uploadModeValue.toLowerCase()) {
                case "full":
                    if (catalog.tableExists(icebergTable)) {
                        LOGGER.info("Starting upload in full mode...");
                        LOGGER.info("Dropping table {} from catalog {}", icebergTable.name(), catalog.name());
                        if (!catalog.dropTable(icebergTable, true)) {
                            LOGGER.error(
                                    "\n=====================\n"
                                    + "Unable to drop table {} from catalog {}"
                                    + "\n=====================\n",
                                    icebergTable.name(), catalog.name());
                            System.exit(1);
                        }
                        icebergTableExists = false;
                    }
                    break;
                case "incremental":
                    LOGGER.info("Starting upload in incremental mode...");
                    LOGGER.info("Add only data to table {} in catalog {}", icebergTable.name(), catalog.name());
                    //TODO Check if we need additional logic for append
                    //TODO in preProcess
                    break;
                case "merge":
                    LOGGER.info("Starting upload in merge mode...");
                    LOGGER.info("Merging data to table {} in catalog {}", icebergTable.name(), catalog.name());
                    //TODO Check if we need additional logic for upsert
                    //TODO Probably need to Check Primary Keys
                    LOGGER.error(
                            "\n=====================\n"
                            + "Merge upload mode is Not Implemented Yet"
                            + "\n=====================\n");
                    System.exit(1);
                    break;
                default:
                    LOGGER.error(
                            "\n=====================\n"
                            + "Unknown upload mode {}. Allowed full (replace), incremental (add only), merge (add/replace/delete)"
                            + "\n=====================\n",
                            uploadModeValue);
                    System.exit(1);
            }

            final Set<String> idColumnNames;
            if (cmd.getOptionValues(OPT_ICEBERG_ID_COLS_SHORT) == null
                    || cmd.getOptionValues(OPT_ICEBERG_ID_COLS_SHORT).length == 0) {
                idColumnNames = null;
            } else {
                idColumnNames = new LinkedHashSet<>();
                for (final String idCol : cmd.getOptionValues(OPT_ICEBERG_ID_COLS_SHORT)) {
                    idColumnNames.add(idCol);
                }
            }
            long maxFileSize;
            if (cmd.hasOption(OPT_ICEBERG_MAX_SIZE_SHORT)) {
                try {
                    maxFileSize = ((Number) cmd.getParsedOptionValue(OPT_ICEBERG_MAX_SIZE_SHORT)).longValue();
                } catch (ParseException pe) {
                    maxFileSize = MAX_FILE_SIZE;
                    LOGGER.error("""
                                 
                                 =====================
                                 Unable to parse value '{}' of option '{}'! Default {} will be used.
                                 =====================
                                 """,
                            cmd.getOptionValue(OPT_ICEBERG_MAX_SIZE_SHORT), OPT_ICEBERG_MAX_SIZE, MAX_FILE_SIZE);
                }
            } else {
                maxFileSize = MAX_FILE_SIZE;
            }

            String defaultNumeric = cmd.getOptionValue("default-number-type", DEFAULT_NUMBER_FORMAT);

            final List<Triple<String, String, Integer>> partColumnNames;
            if (cmd.getOptionValues(OPT_ICEBERG_PARTITION_SHORT) == null
                    || cmd.getOptionValues(OPT_ICEBERG_PARTITION_SHORT).length == 0) {
                partColumnNames = null;
            } else {
                partColumnNames = new ArrayList<>();
                final String[] partParams = cmd.getOptionValues("P");

                if (partParams.length % 2 == 0) {
                    for (int i = 0; i < partParams.length; i += 2) {
                        final String columnName = partParams[i];
                        String partColumnType = partParams[i + 1];
                        String partThirdParamTemp;
                        int partThirdParam = -1;

                        if (StringUtils.contains(partColumnType, ',')) {
                            partThirdParamTemp = StringUtils.substringAfterLast(partColumnType, ",");
                            partColumnType = StringUtils.substringBefore(partColumnType, ",");
                            try {
                                partThirdParam = Integer.parseInt(partThirdParamTemp);

                            } catch (NumberFormatException nfe) {
                                LOGGER.error("""
                                             
                                             =====================
                                             Invalid value {} after the comma in partition type '{}' specified!
                                             The value after the comma should be a valid integer.
                                             Please verify the partition type parameter and try again.
                                             =====================
                                             """,
                                        partThirdParamTemp, partColumnType);
                                System.exit(1);
                            }
                        }
                        partColumnNames.add(new ImmutableTriple<>(columnName, partColumnType, partThirdParam));
                    }
                } else {
                    LOGGER.error("""
                                 
                                 =====================
                                 Unable to parse from command line values of Apache Iceberg Catalog properties!
                                 Please check parameters!
                                 =====================
                                 """);
                    System.exit(1);
                }
            }

            final RdbmsTypeMapper mapper = new RdbmsTypeMapper(defaultNumeric, dataTypeMap);
            final StructAndDataMover sdm = new StructAndDataMover(
                    dbMetaData, sourceSchema, sourceObject, whereClause, isTableOrView, icebergTableExists,
                    catalog, icebergTable, idColumnNames, partColumnNames, maxFileSize, mapper);

            sdm.loadData();

        } catch (SQLException sqle) {
            LOGGER.error("""
                         
                         =====================
                         Caught SQLException {}!
                         Stack trace details:
                         {}
                         
                         =====================
                         """,
                    sqle.getMessage(), ExceptionUtils.getStackTrace(sqle));
            System.exit(1);
        }
    }

    private static void setupCliOptions(final Options options) {

        // Source connection
        final Option sourceJdbcUrl = Option.builder(OPT_SOURCE_JDBC_URL_SHORT)
                .longOpt(OPT_SOURCE_JDBC_URL)
                .hasArg(true)
                .required(true)
                .desc("Oracle JDBC URL of source connection")
                .get();
        options.addOption(sourceJdbcUrl);

        final Option sourceUser = Option.builder(OPT_SOURCE_JDBC_USER_SHORT)
                .longOpt(OPT_SOURCE_JDBC_USER)
                .hasArg(true)
                .required(true)
                .desc("Oracle user for source connection")
                .get();
        options.addOption(sourceUser);

        final Option sourcePassword = Option.builder(OPT_SOURCE_JDBC_PW_SHORT)
                .longOpt(OPT_SOURCE_JDBC_PW)
                .hasArg(true)
                .required(true)
                .desc("Password for source connection")
                .get();
        options.addOption(sourcePassword);

        // Source object description
        final Option sourceSchema = Option.builder(OPT_ICEBERG_SOURCE_SCHEMA_SHORT)
                .longOpt(OPT_ICEBERG_SOURCE_SCHEMA)
                .hasArg(true)
                .required(false)
                .desc("Source schema name. If not specified - value of <source-user> is used")
                .get();
        options.addOption(sourceSchema);

        final Option sourceObject = Option.builder(OPT_ICEBERG_SOURCE_OBJECT_SHORT)
                .longOpt(OPT_ICEBERG_SOURCE_OBJECT)
                .hasArg(true)
                .required(true)
                .desc("The name of source table or view, or valid SQL SELECT statement to query data")
                .get();
        options.addOption(sourceObject);

        final Option whereClause = Option.builder(OPT_WHERE_CLAUSE_SHORT)
                .longOpt(OPT_WHERE_CLAUSE)
                .hasArg(true)
                .required(false)
                .desc("Optional where clause for the <source-object>. Valid only when <source-object> points to table or view.")
                .get();
        options.addOption(whereClause);

        final Option addRowId = Option.builder("r")
                .longOpt("add-rowid-to-iceberg")
                .hasArg(false)
                .required(false)
                .desc("When specified ROWID pseudocolumn is added to destination as VARCHAR column with name ORA_ROW_ID and used as ID. Valid only when <source-object> points to a RDBMS table")
                .get();
        options.addOption(addRowId);

        final Option rowIdColumnName = Option.builder("q")
                .longOpt("rowid-column")
                .hasArg(true)
                .required(false)
                .desc("Specifies the name for the column in destination table storing the source ROWIDs. Default - " + ROWID_KEY)
                .get();
        options.addOption(rowIdColumnName);

        final Option catalogImpl = Option.builder(OPT_ICEBERG_CATALOG_IMPL_SHORT)
                .longOpt(OPT_ICEBERG_CATALOG_IMPL)
                .hasArg(true)
                .required(true)
                .desc("One of "
                        + CATALOG_IMPL_REST + ", "
                        + CATALOG_IMPL_JDBC + ", "
                        + CATALOG_IMPL_HADOOP + ", "
                        + CATALOG_IMPL_HIVE + ", "
                        + CATALOG_IMPL_NESSIE + ", "
                        + CATALOG_IMPL_GLUE + ", "
                        + CATALOG_IMPL_DYNAMODB + ", "
                        + CATALOG_IMPL_SNOWFLAKE
                        + " or full-qualified name of class extending org.apache.iceberg.BaseMetastoreCatalog.")
                .get();
        options.addOption(catalogImpl);

        final Option catalogName = Option.builder("C")
                .longOpt("iceberg-catalog")
                .hasArg(true)
                .required(true)
                .desc("Apache Iceberg Catalog name")
                .get();
        options.addOption(catalogName);

        final Option catalogUri = Option.builder(OPT_ICEBERG_CATALOG_URI_SHORT)
                .longOpt(OPT_ICEBERG_CATALOG_URI)
                .hasArg(true)
                .required(false)
                .desc("Apache Iceberg Catalog URI")
                .get();
        options.addOption(catalogUri);

        final Option catalogWarehouse = Option.builder("H")
                .longOpt("iceberg-warehouse")
                .hasArg(true)
                .required(true)
                .desc("Apache Iceberg warehouse location")
                .get();
        options.addOption(catalogWarehouse);

        final Option catalogProperties = Option.builder(OPT_ICEBERG_PROPS_SHORT)
                .longOpt(OPT_ICEBERG_PROPS)
                .hasArgs()
                .valueSeparator('=')
                .desc("Additional properties for Apache Iceberg catalog implementation")
                .get();
        options.addOption(catalogProperties);

        final Option namespace = Option.builder(OPT_ICEBERG_NAMESPACE_SHORT)
                .longOpt(OPT_ICEBERG_NAMESPACE)
                .hasArg(true)
                .required(false)
                .desc("Apache Iceberg Catalog namespace. If not specified - value of source schema will used.")
                .get();
        options.addOption(namespace);

        final Option icebergTable = Option.builder(OPT_ICEBERG_TABLE_SHORT)
                .longOpt(OPT_ICEBERG_TABLE)
                .hasArg(true)
                .required(false)
                .desc("Apache Iceberg table name. When not specified and <source-object> is view or table, name of <source-object> is used.")
                .get();
        options.addOption(icebergTable);

        final Option idColumns = Option.builder(OPT_ICEBERG_ID_COLS_SHORT)
                .longOpt(OPT_ICEBERG_ID_COLS)
                .hasArgs()
                .desc("Apache Iceberg table identifier column names")
                .get();
        options.addOption(idColumns);

        final Option partitionBy = Option.builder(OPT_ICEBERG_PARTITION_SHORT)
                .longOpt(OPT_ICEBERG_PARTITION)
                .hasArgs()
                .valueSeparator('=')
                .desc("Partitioning definition for table")
                .get();
        options.addOption(partitionBy);

        final Option maxFileSize = Option.builder(OPT_ICEBERG_MAX_SIZE_SHORT)
                .longOpt(OPT_ICEBERG_MAX_SIZE)
                .type(Long.class)
                .hasArg()
                .desc("Max file size. Default - " + MAX_FILE_SIZE)
                .get();
        options.addOption(maxFileSize);

        final Option uploadMode = Option.builder("L")
                .longOpt("upload-mode")
                .hasArg(true)
                .argName("mode")
                .required(false)
                .desc("Specifies the upload mode. Options: full and incremental. Default is full")
                .get();
        options.addOption(uploadMode);

        final Option defaultNumeric = Option.builder("d")
                .longOpt("default-number-type")
                .hasArg(true)
                .required(false)
                .desc("Default NUMERIC precision/scale for ambiguous NUMBER columns. If not specified  - decimal(38,10)")
                .get();
        options.addOption(defaultNumeric);

        final Option dataTypeMap = Option.builder(OPT_DATA_TYPE_MAP_SHORT)
                .longOpt(OPT_DATA_TYPE_MAP)
                .hasArg(true)
                .required(false)
                .desc("Custom mappings from source types to Iceberg types. Example: \"ZONE_CONTROL:NUMBER=integer; %_ID:NUMBER=long; LOCATOR_%:NUMBER=decimal(38,0)\"")
                .get();
        options.addOption(dataTypeMap);
    }

}
