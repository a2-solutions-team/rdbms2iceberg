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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

import oracle.jdbc.OracleResultSet;
import oracle.sql.NUMBER;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.ROWID;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.VARCHAR;
import static solutions.a2.iceberg.DataLoad.ROWID_KEY;
import static solutions.a2.oracle.utils.BinaryUtils.rawToHex;

public class Ora2Iceberg extends Rdbms2IcebergBase implements Rdbms2Iceberg {

    private static final int ORA_17026 = 17026;
    private static final Logger LOGGER = LoggerFactory.getLogger(Ora2Iceberg.class);

    Ora2Iceberg(
            final Connection connection,
            final String sourceSchema,
            final String sourceObject,
            final String whereClause,
            final boolean isTableOrView,
            final boolean rowidPseudoKey) {
        super(connection, sourceSchema, sourceObject, whereClause, isTableOrView, rowidPseudoKey);
    }

    @Override
    public void loadData(
            final Table table,
            final PartitionedFanoutWriter<Record> partitionedFanoutWriter,
            final Map<String, int[]> columnsMap) throws SQLException {
            final PreparedStatement ps;
            if (rowidPseudoKey) {
                ps = connection.prepareStatement(
                        "select ROWIDTOCHAR(ROWID) " + ROWID_KEY + ", T.* from \"" + sourceSchema + "\".\"" + sourceObject + "\" T"
                        + (StringUtils.isBlank(whereClause) ? "" : "\n" + whereClause));
            } else {
                ps = connection.prepareStatement(
                        "select * from \"" + sourceSchema + "\".\"" + sourceObject + "\""
                        + (StringUtils.isBlank(whereClause) ? "" : "\n" + whereClause));
            }
            final OracleResultSet rs = (OracleResultSet) ps.executeQuery();
            while (rs.next()) {
                final GenericRecord record = GenericRecord.create(table.schema());
                for (final Map.Entry<String, int[]> entry : columnsMap.entrySet()) {
                    final String icebergColumn = StringUtils.lowerCase(entry.getKey());
                    switch (entry.getValue()[TYPE_POS]) {
                        case ROWID ->
                            record.setField(icebergColumn, rs.getString(entry.getKey()));
                        case BOOLEAN -> {
                            final boolean dbValue = rs.getBoolean(entry.getKey());
                            record.setField(icebergColumn,
                                    rs.wasNull() ? null : dbValue);
                        }
                        case INTEGER -> {
                            final NUMBER oraInt = rs.getNUMBER(entry.getKey());
                            if (rs.wasNull()) {
                                record.setField(icebergColumn, null);
                            } else {
                                try {
                                    final int intVal = oraInt.intValue();
                                    record.setField(icebergColumn, intVal);
                                } catch (SQLException sqle) {
                                    if (sqle.getErrorCode() == ORA_17026
                                            || Strings.CI.contains(sqle.getMessage(), "Overflow Exception")) {
                                        final StringBuilder sb = new StringBuilder(0x400);
                                        sb
                                                .append("\n=====================\n")
                                                .append("Unable to convert Oracle NUMBER column ")
                                                .append(entry.getKey())
                                                .append(" with value ")
                                                .append(oraInt.stringValue())
                                                .append(" to INTEGER!")
                                                .append("\nDump value of NUMBER column =")
                                                .append(rawToHex(oraInt.getBytes()));
                                        if (entry.getValue()[NULL_POS] == 1) {
                                            record.setField(icebergColumn, null);
                                            sb
                                                    .append("\nSetting value to NULL")
                                                    .append("\n=====================\n");
                                            LOGGER.warn(sb.toString());
                                        } else {
                                            sb.append("\n=====================\n");
                                            LOGGER.error(sb.toString());
                                            throw sqle;
                                        }
                                    } else {
                                        LOGGER.error("""
                                                                                                     
                                                                =====================
                                                                SQL error code={}, SQL state='{}', class='{}'!
                                                                =====================
                                                                """,
                                                sqle.getErrorCode(), sqle.getSQLState(), sqle.getClass().getName());
                                        throw sqle;
                                    }
                                }
                            }
                        }
                        case BIGINT -> {
                            final NUMBER oraLong = rs.getNUMBER(entry.getKey());
                            if (rs.wasNull()) {
                                record.setField(icebergColumn, null);
                            } else {
                                try {
                                    final long longVal = oraLong.longValue();
                                    record.setField(icebergColumn, longVal);
                                } catch (SQLException sqle) {
                                    if (sqle.getErrorCode() == ORA_17026
                                            || Strings.CI.contains(sqle.getMessage(), "Overflow Exception")) {
                                        final StringBuilder sb = new StringBuilder(0x400);
                                        sb
                                                .append("\n=====================\n")
                                                .append("Unable to convert Oracle NUMBER column ")
                                                .append(entry.getKey())
                                                .append(" with value ")
                                                .append(oraLong.stringValue())
                                                .append(" to LONG/BIGINT!")
                                                .append("\nDump value of NUMBER column =")
                                                .append(rawToHex(oraLong.getBytes()));
                                        if (entry.getValue()[NULL_POS] == 1) {
                                            record.setField(icebergColumn, null);
                                            sb
                                                    .append("\nSetting value to NULL")
                                                    .append("\n=====================\n");
                                            LOGGER.warn(sb.toString());
                                        } else {
                                            sb.append("\n=====================\n");
                                            LOGGER.error(sb.toString());
                                            throw sqle;
                                        }
                                    } else {
                                        LOGGER.error("""
                                                     
                                                     =====================
                                                     SQL error code={}, SQL state='{}', class='{}'!
                                                     =====================
                                                     """,
                                                sqle.getErrorCode(), sqle.getSQLState(), sqle.getClass().getName());
                                        throw sqle;
                                    }
                                }
                            }
                        }
                        case NUMERIC -> {
                            final NUMBER oraNum = rs.getNUMBER(entry.getKey());
                            if (rs.wasNull()) {
                                record.setField(icebergColumn, null);
                            } else {
                                if (oraNum.isInf() || oraNum.isNegInf()) {
                                    //TODO
                                    //TODO - key values in output!!!
                                    //TODO
                                    LOGGER.warn("""
                                                
                                                =====================
                                                Value of Oracle NUMBER column {} is {}! Setting value to {}!
                                                =====================
                                                """,
                                            entry.getKey(),
                                            oraNum.isInf() ? "Infinity" : "Negative infinity",
                                            entry.getValue()[NULL_POS] == 1 ? "NULL"
                                            : oraNum.isInf() ? "" + Float.MAX_VALUE : "" + Float.MIN_VALUE);
                                    if (entry.getValue()[NULL_POS] == 1) {
                                        record.setField(icebergColumn, null);
                                    } else if (oraNum.isInf()) {
                                        record.setField(icebergColumn, BigDecimal.valueOf(Float.MAX_VALUE).setScale(entry.getValue()[SCALE_POS]));
                                    } else {
                                        record.setField(icebergColumn, BigDecimal.valueOf(Float.MIN_VALUE).setScale(entry.getValue()[SCALE_POS]));
                                    }
                                } else {
                                    if (oraNum.isNull()) {
                                        record.setField(icebergColumn, null);
                                    } else {
                                        final BigDecimal bd = oraNum
                                                .bigDecimalValue()
                                                .setScale(entry.getValue()[SCALE_POS], RoundingMode.HALF_UP);
                                        if (bd.precision() > entry.getValue()[PRECISION_POS]) {
                                            //TODO
                                            //TODO - key values in output!!!
                                            //TODO
                                            LOGGER.warn("""
                                                        
                                                        =====================
                                                        Precision {} of Oracle NUMBER column {} with value '{}' is greater than allowed precision {}!
                                                        Dump value of NUMBER column ='{}'
                                                        Setting value to {}!
                                                        =====================
                                                        """,
                                                    bd.precision(), entry.getKey(),
                                                    oraNum.stringValue(), entry.getValue()[PRECISION_POS],
                                                    rawToHex(oraNum.getBytes()),
                                                    entry.getValue()[NULL_POS] == 1 ? "NULL" : "" + Float.MAX_VALUE);
                                            if (entry.getValue()[NULL_POS] == 1) {
                                                record.setField(icebergColumn, null);
                                            } else {
                                                //TODO - approximation required, not MAX_VALUE!
                                                record.setField(icebergColumn, BigDecimal.valueOf(Float.MAX_VALUE).setScale(entry.getValue()[SCALE_POS]));
                                            }
                                        } else {
                                            record.setField(icebergColumn, bd);
                                        }
                                    }
                                }
                            }
                        }
                        case FLOAT -> {
                            final float dbValue = rs.getFloat(entry.getKey());
                            record.setField(icebergColumn,
                                    rs.wasNull() ? null : dbValue);
                        }
                        case DOUBLE -> {
                            final double dbValue = rs.getDouble(entry.getKey());
                            record.setField(icebergColumn,
                                    rs.wasNull() ? null : dbValue);
                        }
                        case TIMESTAMP, TIMESTAMP_WITH_TIMEZONE -> {
                            final Timestamp dbValue = rs.getTimestamp(entry.getKey());
                            record.setField(icebergColumn,
                                    rs.wasNull() ? null : dbValue.toLocalDateTime());
                        }
                        case VARCHAR ->
                            record.setField(icebergColumn, rs.getString(entry.getKey()));
                        case NVARCHAR ->
                            record.setField(icebergColumn, rs.getNString(entry.getKey()));
                    }
                }
                try {
                    partitionedFanoutWriter.write(record);
                } catch (IOException ioe) {
                    throw new SQLException(ioe);
                }
            }

            rs.close();
            ps.close();
    }

}
