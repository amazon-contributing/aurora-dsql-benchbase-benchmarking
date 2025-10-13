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
 *
 */

package com.oltpbenchmark.execution;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.XMLConfiguration;

/** Handles anonymization of datasets using differential privacy. */
@Slf4j
public class AnonymizationHandler {
  /**
   * Apply anonymization to specified tables with differential privacy and automatically creates an
   * anonymized copy of the table. Adapts templated query file if sensitive values are present.
   *
   * @param xmlConfig XML configuration
   * @param configFile Configuration file path
   * @throws Exception if anonymization fails
   */
  public void applyAnonymization(XMLConfiguration xmlConfig, String configFile) throws Exception {
    try {
      if (xmlConfig.configurationsAt("/anonymization/table").size() > 0) {
        String templatesPath = "";
        if (xmlConfig.containsKey("query_templates_file")) {
          templatesPath = xmlConfig.getString("query_templates_file");
        }

        log.info("Starting the Anonymization process");
        log.info(BenchmarkConstants.SINGLE_LINE);

        String osCommand =
            System.getProperty("os.name").startsWith("Windows") ? "python" : "python3";

        ProcessBuilder processBuilder =
            new ProcessBuilder(
                osCommand, "scripts/anonymization/src/anonymizer.py", configFile, templatesPath);

        // Redirect Output stream of the script to get live feedback
        processBuilder.inheritIO();
        Process process = processBuilder.start();
        int exitCode = process.waitFor();

        if (exitCode != 0) {
          throw new Exception("Anonymization program exited with a non-zero status code");
        }

        log.info("Finished the Anonymization process for all tables");
        log.info(BenchmarkConstants.SINGLE_LINE);
      }
    } catch (Exception e) {
      log.error("Anonymization failed", e);
      throw e;
    }
  }
}
