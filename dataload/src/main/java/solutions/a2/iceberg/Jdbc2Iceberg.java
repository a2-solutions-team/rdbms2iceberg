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


public class Jdbc2Iceberg extends Rdbms2IcebergBase implements Rdbms2Iceberg {

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
                            final BigDecimal dbValue = rs.getBigDecimal(entry.getKey());
                            record.setField(icebergColumn,
                                    rs.wasNull() ? null : dbValue);
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
