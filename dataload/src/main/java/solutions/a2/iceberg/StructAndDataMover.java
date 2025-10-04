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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.snowflake.client.jdbc.SnowflakeSQLException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.sql.Types.ROWID;
import static solutions.a2.iceberg.DataLoad.ROWID_ORA;
import static solutions.a2.iceberg.DataLoad.ROWID_KEY;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_IDENTITY;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_BUCKET;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_TRUNCATE;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_YEAR;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_MONTH;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_DAY;
import static solutions.a2.iceberg.DataLoad.PARTITION_TYPE_HOUR;
import static solutions.a2.iceberg.Rdbms2IcebergBase.TYPE_POS;
import static solutions.a2.iceberg.Rdbms2IcebergBase.PRECISION_POS;
import static solutions.a2.iceberg.Rdbms2IcebergBase.SCALE_POS;
import static solutions.a2.iceberg.Rdbms2IcebergBase.NULL_POS;
import static solutions.a2.iceberg.Rdbms2IcebergBase.INFO_SIZE;

/**
 *
 * @author <a href="mailto:averemee@a2.solutions">Aleksei Veremeev</a>
 */
public class StructAndDataMover {

    private static final Logger LOGGER = LoggerFactory.getLogger(StructAndDataMover.class);

    private final boolean isTableOrView;
    private final Map<String, int[]> columnsMap;
    private final long targetFileSize;
    private final boolean rowidPseudoKey;
    private final Rdbms2Iceberg dataLoader;
    private Table table;

    StructAndDataMover(
            final DatabaseMetaData dbMetaData,
            final String sourceSchema,
            final String sourceObject,
            final String whereClause,
            final boolean isTableOrView,
            final boolean icebergTableExists,
            final Catalog catalog,
            final TableIdentifier icebergTable,
            final Set<String> idColumnNames,
            final List<Triple<String, String, Integer>> partitionDefs,
            final long targetFileSize,
            final RdbmsTypeMapper mapper) throws SQLException {
        columnsMap = new HashMap<>();
        this.isTableOrView = isTableOrView;
        this.targetFileSize = targetFileSize;

        final String sourceCatalog;
        if (isTableOrView) {
            final ResultSet tables = dbMetaData.getTables(null, sourceSchema, sourceObject, null);
            if (tables.next()) {
                sourceCatalog = tables.getString("TABLE_CAT");
                LOGGER.info("Working with {} {}.{}",
                        tables.getString("TABLE_TYPE"), sourceSchema, sourceObject);
            } else {
                LOGGER.error("""
                                             
                                             =====================
                                             Unable to access {}.{} !
                                             =====================
                                             """,
                        sourceSchema, sourceObject);
                throw new SQLException();
            }
            tables.close();

            final List<Types.NestedField> allColumns = new ArrayList<>();
            final Set<Integer> pkIds = new LinkedHashSet<>();
            int columnId;

            final boolean idColumnsPresent = idColumnNames != null && !idColumnNames.isEmpty();
            if (isTableOrView
                    && idColumnsPresent
                    && idColumnNames.size() == 1
                    && Strings.CI.equals(idColumnNames.iterator().next(), ROWID_ORA)) {
                rowidPseudoKey = true;
                columnId = 1;
                allColumns.add(
                        Types.NestedField.required(columnId, StringUtils.lowerCase(ROWID_KEY), Types.StringType.get()));
                pkIds.add(columnId);
                final int[] typeAndScale = new int[INFO_SIZE];
                typeAndScale[TYPE_POS] = ROWID;
                typeAndScale[PRECISION_POS] = 0;
                typeAndScale[SCALE_POS] = 0;
                typeAndScale[NULL_POS] = 0;
                columnsMap.put(ROWID_KEY, typeAndScale);
            } else {
                rowidPseudoKey = false;
                columnId = 0;
            }

            final ResultSet columns = dbMetaData.getColumns(sourceCatalog, sourceSchema, sourceObject, "%");
            while (columns.next()) {
                final String columnName = columns.getString("COLUMN_NAME");
                final int jdbcType = columns.getInt("DATA_TYPE");
                final boolean nullable = Strings.CS.equals("YES", columns.getString("IS_NULLABLE"));
                final int precision = columns.getInt("COLUMN_SIZE");
                final int scale = columns.getInt("DECIMAL_DIGITS");
                LOGGER.debug("Source Metadata info {}:{}({},{})",
                        columnName, JdbcUtils.getTypeName(jdbcType), precision, scale);

                boolean addColumn = false;

                final Pair<Integer, Type> remapped = mapper.icebergType(columnName, jdbcType, precision, scale);
                final Type type = remapped.getRight();
                final int mappedType = remapped.getLeft();

                final int finalPrecision;
                final int finalScale;
                if (type instanceof DecimalType) {
                    final DecimalType decimalType = (DecimalType) type;
                    finalPrecision = decimalType.precision();
                    finalScale = decimalType.scale();
                } else {
                    finalPrecision = precision;
                    finalScale = scale;
                }

                LOGGER.info("Column map info {}:{}={}({},{})",
                        columnName, JdbcUtils.getTypeName(jdbcType), JdbcUtils.getTypeName(mappedType), finalPrecision, finalScale);

                addColumn = true;
                if (addColumn) {
                    final int[] typeAndScale = new int[INFO_SIZE];
                    typeAndScale[TYPE_POS] = mappedType; //TODO !!!
                    typeAndScale[PRECISION_POS] = finalPrecision;
                    typeAndScale[SCALE_POS] = finalScale;
                    typeAndScale[NULL_POS] = nullable ? 1 : 0;
                    columnsMap.put(columnName, typeAndScale);
                    columnId++;
                    if (!nullable || (idColumnsPresent && idColumnNames.contains(columnName))) {
                        if (nullable) {
                            LOGGER.error("Unable to add nullable column {} as equality delete column!", columnName);
                            System.exit(1);
                        } else {
                            allColumns.add(
                                    Types.NestedField.required(columnId, StringUtils.lowerCase(columnName), type));
                        }
                    } else {
                        allColumns.add(
                                Types.NestedField.optional(columnId, StringUtils.lowerCase(columnName), type));
                    }
                    if (idColumnsPresent) {
                        if (idColumnNames.contains(columnName) /* && !nullable */) {
                            pkIds.add(columnId);
                        }
                    }
                }
            }

            final Schema schema = pkIds.isEmpty() ? new Schema(allColumns) : new Schema(allColumns, pkIds);
            final PartitionSpec spec;
            if (partitionDefs != null) {

                PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);

                String partTypeTemp, partFieldTemp;
                Integer partParamTemp;

                for (Triple<String, String, Integer> partitionDef : partitionDefs) {

                    partTypeTemp = partitionDef.getMiddle().toUpperCase();
                    partFieldTemp = StringUtils.lowerCase(partitionDef.getLeft());
                    partParamTemp = partitionDef.getRight();

                    LOGGER.info("Column map info {} {} {}",
                            partTypeTemp, partFieldTemp, partParamTemp);

                    switch (partTypeTemp) {
                        case PARTITION_TYPE_IDENTITY -> specBuilder.identity(partFieldTemp);
                        case PARTITION_TYPE_YEAR -> specBuilder.year(partFieldTemp);
                        case PARTITION_TYPE_MONTH -> specBuilder.month(partFieldTemp);
                        case PARTITION_TYPE_DAY -> specBuilder.day(partFieldTemp);
                        case PARTITION_TYPE_HOUR -> specBuilder.hour(partFieldTemp);
                        case PARTITION_TYPE_BUCKET -> specBuilder.bucket(partFieldTemp, partParamTemp);
                        case PARTITION_TYPE_TRUNCATE -> specBuilder.truncate(partFieldTemp, partParamTemp);
                        default -> {
                            LOGGER.error("""
                                =====================
                                Invalid partition type '{}' specified!
                                Supported partition types are: `identity`, `year`, `month`, `day`, `bucket`, `truncate`.
                                Please verify the partition type and try again.
                                =====================
                                """,
                                    partTypeTemp);
                            System.exit(1);
                        }
                    }
                }
                spec = specBuilder.build();
            } else {
                spec = PartitionSpec.unpartitioned();
            }

            try {
                if (!icebergTableExists) {
                    try {
                        table = catalog.createTable(
                                icebergTable,
                                schema,
                                spec);
                    } catch (Exception e) {
                        if (e instanceof NoSuchTableException
                                && e.getCause() != null
                                && e instanceof SnowflakeSQLException) {
                            //TODO table creation through SF JDBC?
                            LOGGER.error("""

                                    =====================
                                    Please create Snowflake Iceberg table manually!
                                    Stack trace:
                                    {}
                                    
                                    =====================
                                """,
                                    ExceptionUtils.getFullStackTrace(e));
                            System.exit(1);
                        } else {
                            throw e;
                        }
                    }
                } else {
                    table = catalog.loadTable(icebergTable);
                }
            } catch (NoSuchNamespaceException nsne) {
                if (Strings.CS.endsWith(catalog.getClass().getName(), "HiveCatalog")) {
                    LOGGER.error("""
                                                             
                                                             =====================
                                                             Hive database '{}' does not exists. Please create it using
                                                             \tCREATE DATABASE [IF NOT EXISTS] {} [DATABASE-OPTIONS];
                                                             Full error stack:
                                                             {}
                                                             =====================
                                                             """,
                            icebergTable.namespace().toString(), icebergTable.namespace().toString(),
                            ExceptionUtils.getFullStackTrace(nsne));
                } else {
                    LOGGER.error("""
                                                             
                                                             =====================
                                                             org.apache.iceberg.exceptions.NoSuchNamespaceException!
                                                             Full error stack:
                                                             {}
                                                             =====================
                                                             """,
                            ExceptionUtils.getFullStackTrace(nsne));
                }
                throw nsne;
            }
        } else {
            //TODO
            //TODO
            //TODO
            throw new SQLException("Not supported yet!");
        }
        dataLoader = Rdbms2IcebergFactory.get(dbMetaData.getConnection(), sourceSchema, sourceObject, whereClause, isTableOrView, rowidPseudoKey);
    }

    void loadData() throws SQLException {

        final long startMillis = System.currentTimeMillis();
        int partitionId = 1, taskId = 1;
        final GenericAppenderFactory af = new GenericAppenderFactory(table.schema(), table.spec());
        final OutputFileFactory off = OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(FileFormat.PARQUET)
                .build();
        final PartitionKey partitionKey = new PartitionKey(table.spec(), table.spec().schema());
        final InternalRecordWrapper recordWrapper = new InternalRecordWrapper(table.schema().asStruct());

        if (isTableOrView) {

            PartitionedFanoutWriter<Record> partitionedFanoutWriter = new PartitionedFanoutWriter<Record>(
                    table.spec(),
                    //TODO - only parquet?
                    FileFormat.PARQUET,
                    af, off, table.io(),
                    targetFileSize) {
                @Override
                protected PartitionKey partition(Record record) {
                    partitionKey.partition(recordWrapper.wrap(record));
                    return partitionKey;
                }
            };

            dataLoader.loadData(table, partitionedFanoutWriter, columnsMap);

            AppendFiles appendFiles = table.newAppend();
            // submit datafiles to the table
            try {
                Arrays.stream(partitionedFanoutWriter.dataFiles()).forEach(appendFiles::appendFile);
            } catch (IOException ioe) {
                throw new SQLException(ioe);
            }
            Snapshot newSnapshot = appendFiles.apply();
            appendFiles.commit();

            final StringBuilder sb = new StringBuilder(0x400);
            sb
                    .append("\n=====================\n")
                    .append("\tSummary data for the operation that produced new snapshot")
                    .append("\nElapsed time: ")
                    .append((System.currentTimeMillis() - startMillis))
                    .append(" ms");
            newSnapshot.summary().forEach((k, v)
                    -> sb
                            .append('\n')
                            .append(k)
                            .append("\t:")
                            .append(v));
            sb.append("\n=====================\n");
            LOGGER.info(sb.toString());

        } else {
            //TODO
            //TODO  Select Statement Support Here (Not Table/View)
            //TODO
            throw new SQLException("Select Statement Not supported yet!");
        }
    }

}
