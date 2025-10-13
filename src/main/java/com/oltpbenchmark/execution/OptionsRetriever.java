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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.configuration2.XMLConfiguration;

/**
 * Helper class to retrieve options from either command line arguments or XML configuration. Command
 * line arguments take precedence over XML configuration values.
 */
public class OptionsRetriever {

  private final CommandLine argsLine;
  private final XMLConfiguration xmlConfig;

  public OptionsRetriever(CommandLine argsLine, XMLConfiguration xmlConfig) {
    this.argsLine = argsLine;
    this.xmlConfig = xmlConfig;
  }

  /**
   * Get a string value, preferring command line over XML config
   *
   * @param key The option key
   * @return The value or null if not found
   */
  public String getString(String key) {
    if (argsLine.hasOption(key)) {
      return argsLine.getOptionValue(key);
    }
    return xmlConfig.getString(key);
  }

  /**
   * Get a string value with a default
   *
   * @param key The option key
   * @param defaultValue The default value if not found
   * @return The value or default
   */
  public String getString(String key, String defaultValue) {
    if (argsLine.hasOption(key)) {
      return argsLine.getOptionValue(key);
    }
    return xmlConfig.getString(key, defaultValue);
  }

  /**
   * Get an integer value, preferring command line over XML config
   *
   * @param key The option key
   * @return The integer value
   * @throws NumberFormatException if the value cannot be parsed as an integer
   */
  public int getInt(String key) {
    if (argsLine.hasOption(key)) {
      return Integer.parseInt(argsLine.getOptionValue(key));
    }
    return xmlConfig.getInt(key);
  }

  /**
   * Get an integer value with a default
   *
   * @param key The option key
   * @param defaultValue The default value if not found
   * @return The integer value or default
   */
  public int getInt(String key, int defaultValue) {
    if (argsLine.hasOption(key)) {
      return Integer.parseInt(argsLine.getOptionValue(key));
    }
    return xmlConfig.getInt(key, defaultValue);
  }

  /**
   * Get a double value, preferring command line over XML config
   *
   * @param key The option key
   * @return The double value
   * @throws NumberFormatException if the value cannot be parsed as a double
   */
  public double getDouble(String key) {
    if (argsLine.hasOption(key)) {
      return Double.parseDouble(argsLine.getOptionValue(key));
    }
    return xmlConfig.getDouble(key);
  }

  /**
   * Get a double value with a default
   *
   * @param key The option key
   * @param defaultValue The default value if not found
   * @return The double value or default
   */
  public double getDouble(String key, double defaultValue) {
    if (argsLine.hasOption(key)) {
      return Double.parseDouble(argsLine.getOptionValue(key));
    }
    return xmlConfig.getDouble(key, defaultValue);
  }

  /**
   * Get a boolean value with a default
   *
   * @param key The option key
   * @param defaultValue The default value if not found
   * @return The boolean value or default
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    if (argsLine.hasOption(key)) {
      return Boolean.parseBoolean(argsLine.getOptionValue(key));
    }
    return xmlConfig.getBoolean(key, defaultValue);
  }

  /**
   * Return boolean based on the presence of option (ignoring the option value).
   *
   * @param key The option key
   * @param defaultValue The default value if not found
   * @return The boolean value or default
   */
  public boolean getBooleanWithoutArg(String key, boolean defaultValue) {
    if (argsLine.hasOption(key)) {
      return true;
    }
    return xmlConfig.getBoolean(key, defaultValue);
  }
}
