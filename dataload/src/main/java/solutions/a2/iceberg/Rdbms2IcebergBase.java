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

import java.sql.Connection;

public abstract class Rdbms2IcebergBase {

    static final int TYPE_POS = 0;
    static final int PRECISION_POS = 1;
    static final int SCALE_POS = 2;
    static final int NULL_POS = 3;
    static final int INFO_SIZE = 4;

    protected final Connection connection;
    protected final String sourceSchema;
    protected final String sourceObject;
    protected final String whereClause;
    protected final boolean isTableOrView;
    protected final boolean rowidPseudoKey;

    Rdbms2IcebergBase(
            final Connection connection,
            final String sourceSchema,
            final String sourceObject,
            final String whereClause,
            final boolean isTableOrView,
            final boolean rowidPseudoKey) {
        this.connection = connection;
        this.sourceSchema = sourceSchema;
        this.sourceObject = sourceObject;
        this.whereClause = whereClause;
        this.isTableOrView = isTableOrView;
        this.rowidPseudoKey = rowidPseudoKey;
    }
}
