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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import static solutions.a2.iceberg.JdbcUtils.getTypeName;


public class Jdbc2Iceberg extends Rdbms2IcebergBase implements Rdbms2Iceberg {

    private static final Logger LOGGER = LoggerFactory.getLogger(Jdbc2Iceberg.class);

    Jdbc2Iceberg(
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
            if (StringUtils.isBlank(sourceSchema)) {
                ps = connection.prepareStatement(
                        "select * from \"" + sourceObject + "\""
                        + (StringUtils.isBlank(whereClause) ? "" : "\n" + whereClause));
            } else {
                ps = connection.prepareStatement(
                        "select * from \"" + sourceSchema + "\".\"" + sourceObject + "\""
                        + (StringUtils.isBlank(whereClause) ? "" : "\n" + whereClause));
            }
            final ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                final GenericRecord record = GenericRecord.create(table.schema());
                for (final Map.Entry<String, int[]> entry : columnsMap.entrySet()) {
                    try {
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
                                final int dbValue = rs.getInt(entry.getKey());
                                record.setField(icebergColumn,
                                        rs.wasNull() ? null : dbValue);
                            }
                            case BIGINT -> {
                                final long dbValue = rs.getLong(entry.getKey());
                                record.setField(icebergColumn,
                                        rs.wasNull() ? null : dbValue);
                            }
                            case NUMERIC -> {
                                BigDecimal dbValue = rs.getBigDecimal(entry.getKey());
                                if (dbValue != null) {
                                    if (dbValue.scale() != entry.getValue()[SCALE_POS]) {
                                        dbValue = dbValue.setScale(entry.getValue()[SCALE_POS], RoundingMode.HALF_UP);
                                    }
                                    if (dbValue.precision() > entry.getValue()[PRECISION_POS]) {
                                        LOGGER.warn("Incorrect precision {} for column {}. Value of {} is changed to NULL",
                                                dbValue.precision(), entry.getValue()[PRECISION_POS], dbValue);
                                        dbValue = null;
                                    }
                                    record.setField(icebergColumn,
                                            dbValue == null ? null : dbValue);
                                } else
                                    record.setField(icebergColumn, null);
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
                    } catch (SQLException sqle) {
                            LOGGER.error("""
                                
                                =====================
                                ErrorCode={}, SQLState='{}', Message='{}'
                                during moving of column {} with type {}
                                =====================
                                """,
                                    sqle.getErrorCode(), sqle.getSQLState(), sqle.getMessage(),
                                    entry.getKey(), getTypeName(entry.getValue()[TYPE_POS]));
                        throw sqle;
                    }
                }
                try {
                    partitionedFanoutWriter.write(record);
                } catch (Exception e) {
                    StringBuilder sb = new StringBuilder(0x400);
                    sb
                            .append("\n=====================\n")
                            .append(e.getMessage())
                            .append("\nRecord content:");
                    for (final Map.Entry<String, int[]> entry : columnsMap.entrySet()) {
                        final String icebergColumn = StringUtils.lowerCase(entry.getKey());
                        sb
                                .append('\n')
                                .append(icebergColumn)
                                .append(" with type '")
                                .append(getTypeName(entry.getValue()[TYPE_POS]))
                                .append("' precision=")
                                .append(entry.getValue()[PRECISION_POS])
                                .append(", scale=")
                                .append(entry.getValue()[SCALE_POS])
                                .append(", value=[")
                                .append(record.getField(icebergColumn))
                                .append("]");
                    }

                    sb.append("\n=====================\n");
                    LOGGER.error(sb.toString());
                    throw new SQLException(e);
                }
            }

            rs.close();
            ps.close();
    }


}
