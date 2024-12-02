package com.oltpbenchmark.util;

import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.providers.AwsRegionProviderChain;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.dsql.DsqlUtilities;

public class IAMUtil {
  // Default token validity is one hour
  private static final Duration DEFAULT_VALIDITY = Duration.ofHours(1);

  private static final String ADMIN_USERNAME = "admin";

  public static String generateAuroraDsqlPasswordToken(String url, String username) {
    return generateAuroraDsqlPasswordToken(
        url,
        username,
        DefaultCredentialsProvider.builder().reuseLastProviderEnabled(false).build(),
        DefaultAwsRegionProviderChain.builder().build());
  }

  public static String generateAuroraDsqlPasswordToken(
      String url,
      String username,
      AwsCredentialsProvider credentialsProvider,
      AwsRegionProviderChain regionProvider) {
    DsqlUtilities utilities =
        DsqlUtilities.builder()
            .region(regionProvider.getRegion())
            .credentialsProvider(credentialsProvider)
            .build();

    try {
      IAMUtil.validateUrl(url);
      String host = url.split("//")[1].split(":")[0];
      return username.equals(ADMIN_USERNAME)
          ? utilities.generateDbConnectAdminAuthToken(
              builder ->
                  builder
                      .hostname(host)
                      .region(regionProvider.getRegion())
                      .expiresIn(DEFAULT_VALIDITY))
          : utilities.generateDbConnectAuthToken(
              builder ->
                  builder
                      .hostname(host)
                      .region(regionProvider.getRegion())
                      .expiresIn(DEFAULT_VALIDITY));
    } catch (SdkClientException e) {
      throw new RuntimeException(e);
    }
  }

  private static void validateUrl(String url) {
    String regex = "(^jdbc:postgresql:\\/\\/[a-zA-Z0-9_.-]+(:[0-9]{1,9})?\\/[a-zA-Z]*\\\\?.+$)1?";
    if (!url.matches(regex)) {
      throw new RuntimeException("Invalid JDBC url provided!");
    }
  }
}
