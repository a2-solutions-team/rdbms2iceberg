/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under
 * the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package solutions.a2.iceberg;

import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.FLOAT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.TINYINT;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.BIGINT;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.DATE;
import static java.sql.Types.TIME;
import static java.sql.Types.BINARY;
import static solutions.a2.iceberg.JdbcUtils.isNumeric;

public class RdbmsTypeMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(RdbmsTypeMapper.class);
    private static final int ICEBERG_MAX_PRECISION = 0x26;

    private final Map<String, Triple<Integer, Integer, Integer>> exactOverrides = new HashMap<>();
    private final Map<String, Triple<Integer, Integer, Integer>> patternOverrides = new HashMap<>();
    private int defaultPrecision = ICEBERG_MAX_PRECISION;
    private int defaultScale = 0x0A;

    RdbmsTypeMapper(final String defaultNumeric, final String dataTypeMap) {
        if (StringUtils.isNotBlank(dataTypeMap)) {
            final String[] overrideArray = StringUtils.split(dataTypeMap, ';');
            for (final String overrideSpec : overrideArray) {
                if (StringUtils.isNotBlank(StringUtils.trim(overrideSpec)) && StringUtils.contains(overrideSpec, ':')) {
                    final String columnOrPattern = StringUtils.trim(StringUtils.substringBefore(StringUtils.trim(overrideSpec), ':'));
                    final String overrideData = StringUtils.substringAfter(StringUtils.trim(overrideSpec), ':');
                    if (StringUtils.isNotBlank(columnOrPattern) &&
                            StringUtils.isNotBlank(overrideData)) {
                        if (columnOrPattern.contains("%")) {
                            patternOverrides.put(columnOrPattern, override(StringUtils.substringAfter(StringUtils.trim(overrideSpec), ':')));
                        } else {
                            exactOverrides.put(columnOrPattern, override(StringUtils.substringAfter(StringUtils.trim(overrideSpec), ':')));
                        }
                    } else {
                        //TODO - message
                    }
                } else {
                    //TODO - message
                }
            }
        }
        if (StringUtils.isNotBlank(defaultNumeric)) {
            final Triple<Integer, Integer, Integer> defaultNum = override(defaultNumeric);
            if (defaultNum != null) {
                defaultPrecision = defaultNum.getMiddle();
                defaultScale = defaultNum.getRight();
            }
        }
    }

    Pair<Integer, Type> icebergType(final String columnName, final int jdbcType, final int precision, final int scale) {
        //TODO - type compatibility required!
        if (isNumeric(jdbcType)) {
            if (exactOverrides.containsKey(StringUtils.upperCase(columnName))) {        
                final Triple<Integer, Integer, Integer> typeDef = exactOverrides.get(StringUtils.upperCase(columnName));
                return icebergType(typeDef.getLeft(), typeDef.getMiddle(), typeDef.getRight());
            } else {
                String result = null;
                for (String pattern : patternOverrides.keySet()) {
                    if (Strings.CS.startsWith(pattern, "%") &&
                            Strings.CS.endsWith(columnName, StringUtils.substringAfter(pattern, '%'))) {
                        // LIKE '%SOMETHING'
                        result = pattern;
                        break;
                    }
                    if (Strings.CS.endsWith(pattern, "%") &&
                            Strings.CS.startsWith(columnName, StringUtils.substringBefore(pattern, '%'))) {
                        // LIKE 'SOMETHING%'
                        result = pattern;
                        break;
                    }
                }
                if (result != null) {
                        final Triple<Integer, Integer, Integer> typeDef = patternOverrides.get(result);
                        return icebergType(typeDef.getLeft(), typeDef.getMiddle(), typeDef.getRight());
                } else {
                    return icebergType(jdbcType, precision, scale);
                }
            }  
        } else {
            return icebergType(jdbcType, precision, scale);
        }
    }

	private Pair<Integer, Type> icebergType(final int jdbcType, final int precision, final int scale) {
		switch (jdbcType) {
		case FLOAT -> {
                    return new ImmutablePair<>(FLOAT, Types.FloatType.get());
                }
		case DOUBLE -> {
                    return new ImmutablePair<>(DOUBLE, Types.DoubleType.get());
                }
		case BOOLEAN -> {
                    return new ImmutablePair<>(BOOLEAN, Types.BooleanType.get());
                }
		case TINYINT, SMALLINT, INTEGER -> {
                    return new ImmutablePair<>(INTEGER, Types.IntegerType.get());
                }
		case BIGINT -> {
                    return new ImmutablePair<>(BIGINT, Types.LongType.get());
                }
		case NUMERIC -> {
                    if ((scale == 0 && precision == 0) || (scale < 0 && precision < 1))
                        return new ImmutablePair<>(NUMERIC, Types.DecimalType.of(defaultPrecision, defaultScale));
                    else if (scale == 0 && precision < 0x0A)
                        return new ImmutablePair<>(INTEGER, Types.IntegerType.get());
                    else if (scale == 0 && precision < 0x13)
                        return new ImmutablePair<>(BIGINT, Types.LongType.get());
                    else if (precision <= ICEBERG_MAX_PRECISION && scale < precision)
                        return new ImmutablePair<>(NUMERIC, Types.DecimalType.of(precision, scale));
                    else
                        return new ImmutablePair<>(NUMERIC, Types.DecimalType.of(defaultPrecision, defaultScale));
                }
		case VARCHAR -> {
                    return new ImmutablePair<>(VARCHAR, Types.StringType.get());
                }
		case NVARCHAR -> {
                    return new ImmutablePair<>(NVARCHAR, Types.StringType.get());
                }
		case TIMESTAMP -> {
                    return new ImmutablePair<>(TIMESTAMP, Types.TimestampType.withoutZone());
                }
		case TIMESTAMP_WITH_TIMEZONE -> {
                    return new ImmutablePair<>(TIMESTAMP_WITH_TIMEZONE, Types.TimestampType.withZone());
                }
		case DATE -> {
                    return new ImmutablePair<>(DATE, Types.DateType.get());
                }
		case TIME -> {
                    return new ImmutablePair<>(TIME, Types.TimeType.get());
                }
		case BINARY -> {
                    return new ImmutablePair<>(BINARY, Types.BinaryType.get());
                }
                default -> {
                    return new ImmutablePair<>(VARCHAR, Types.StringType.get());
                }
		}
	}

	private Triple<Integer, Integer, Integer> override(final String overrideSpec) {
		final String sourceType = StringUtils.substringBefore(overrideSpec, '=');
		final String targetType = StringUtils.substringAfter(overrideSpec, '=');
		if (Strings.CI.equals(sourceType, "NUMBER") ||
				Strings.CI.equals(sourceType, "FLOAT")) {
			if (Strings.CI.startsWith(targetType, "DECIMAL") ||
					Strings.CI.startsWith(targetType, "NUMERIC") ||
					Strings.CI.startsWith(targetType, "NUMBER")) {
				final String[] overrideArgs = StringUtils.split(
						StringUtils.substringBetween(targetType, "(", ")"),
						',');
				if (overrideArgs == null || overrideArgs.length != 2) {
					LOGGER.error("""
                                                     
                                                     =====================
                                                     Unable to parse override '{}'! This override definition has been ignored!
                                                     =====================
                                                     """,
							overrideSpec);
					return null;
				} else {
					int precision = defaultPrecision;
					try {
						precision = Integer.parseInt(overrideArgs[0]);
					} catch (NumberFormatException nfe) {
						LOGGER.error("""
                                                             
                                                             =====================
                                                             Unable to parse precision '{}' in override specification '{}'! Default value of {} will be used for precision!
                                                             =====================
                                                             """,
								overrideArgs[0], overrideSpec, defaultPrecision);
					}
					int scale = defaultScale;
					try {
						scale = Integer.parseInt(overrideArgs[1]);
					} catch (NumberFormatException nfe) {
						LOGGER.error("""
                                                             
                                                             =====================
                                                             Unable to parse scale '{}' in override specification '{}'! Default value of {} will be used for scale!
                                                             =====================
                                                             """,
								overrideArgs[1], overrideSpec, defaultScale);
					}
					if (scale >= precision) {
						LOGGER.error("""
                                                             
                                                             =====================
                                                             Scale '{}' can't be greater than a precision '{}' in override specification '{}'! Default values of ({},{}) will be used for precision, scale !
                                                             =====================
                                                             """,
								scale, precision, overrideSpec, defaultPrecision, defaultScale);
					}
					return ImmutableTriple.of(NUMERIC, precision, scale);
				}
			} else if (Strings.CI.equals(targetType, "LONG") ||
					Strings.CI.equals(targetType, "BIGINT")) {
				return ImmutableTriple.of(BIGINT, 0x12, 0);
			} else if (Strings.CI.equals(targetType, "INT") ||
				Strings.CI.equals(targetType, "INTEGER")) {
				return ImmutableTriple.of(INTEGER, 0x09, 0);
			} else if (Strings.CI.equals(targetType, "DOUBLE")) {
				return ImmutableTriple.of(DOUBLE, 0, 0);
			} else if (Strings.CI.equals(targetType, "FLOAT")) {
				return ImmutableTriple.of(FLOAT, 0, 0);
			} else {
				LOGGER.error("""
                                             
                                             =====================
                                             Unable to parse override '{}'! this override definition is ignored!
                                             =====================
                                             """,
						overrideSpec);
				return null;
			}
		} else {
			LOGGER.error("""
                                     
                                     =====================
                                     Unable to parse override '{}'! This override definition has been ignored!
                                     =====================
                                     """,
					overrideSpec);
			return null;
		}

	}


}