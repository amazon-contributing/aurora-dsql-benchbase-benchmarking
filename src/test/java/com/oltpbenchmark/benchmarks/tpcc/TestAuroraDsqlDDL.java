/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oltpbenchmark.benchmarks.tpcc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;

public class TestAuroraDsqlDDL {

  private static final String DDL_RESOURCE = "/benchmarks/tpcc/ddl-auroradsql.sql";

  @Test
  public void testForeignKeyConstraints() throws IOException {
    String ddl;
    try (InputStream stream = getClass().getResourceAsStream(DDL_RESOURCE)) {
      assertTrue("Missing " + DDL_RESOURCE, stream != null);
      ddl = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    String normalizedDdl = ddl.toLowerCase().replaceAll("\\s+", " ");
    List<String> constraints =
        List.of(
            "foreign key (s_w_id) references warehouse (w_id) on delete cascade",
            "foreign key (s_i_id) references item (i_id) on delete cascade",
            "foreign key (d_w_id) references warehouse (w_id) on delete cascade",
            "foreign key (c_w_id, c_d_id) references district (d_w_id, d_id) on delete cascade",
            "foreign key (h_c_w_id, h_c_d_id, h_c_id) references customer (c_w_id, c_d_id, c_id) on delete cascade",
            "foreign key (h_w_id, h_d_id) references district (d_w_id, d_id) on delete cascade",
            "foreign key (o_w_id, o_d_id, o_c_id) references customer (c_w_id, c_d_id, c_id) on delete cascade",
            "foreign key (no_w_id, no_d_id, no_o_id) references oorder (o_w_id, o_d_id, o_id) on delete cascade",
            "foreign key (ol_w_id, ol_d_id, ol_o_id) references oorder (o_w_id, o_d_id, o_id) on delete cascade",
            "foreign key (ol_supply_w_id, ol_i_id) references stock (s_w_id, s_i_id) on delete cascade");

    assertEquals(
        "Unexpected number of Aurora DSQL foreign keys",
        constraints.size(),
        normalizedDdl.split("foreign key \\(", -1).length - 1);

    for (String constraint : constraints) {
      assertTrue(
          "Missing Aurora DSQL constraint: " + constraint, normalizedDdl.contains(constraint));
    }
  }
}
