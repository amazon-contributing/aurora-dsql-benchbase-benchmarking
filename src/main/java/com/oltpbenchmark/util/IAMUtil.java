package com.oltpbenchmark.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4PresignerParams;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.AwsRegionProviderChain;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public class IAMUtil {
  // Default token validity is one hour
  private static final Duration DEFAULT_VALIDITY = Duration.ofHours(1);

  private static final String ADMIN_USERNAME = "admin";

  private static final String SIGNING_NAME = "dsql";

  private static final String DB_CONNECT_ADMIN = "DbConnectAdmin";

  private static final String DB_CONNECT = "DbConnect";

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
    try {
      IAMUtil.validateUrl(url);
      String host = url.split("//")[1].split(":")[0];

      Clock now = Clock.systemUTC();
      Region region = regionProvider.getRegion();
      if (region == null) region = Region.US_EAST_1;

      Aws4Signer signer = Aws4Signer.create();
      Aws4PresignerParams presignerParams =
          Aws4PresignerParams.builder()
              .signingName(SIGNING_NAME)
              .signingRegion(region)
              .awsCredentials(credentialsProvider.resolveCredentials())
              .signingClockOverride(now)
              .expirationTime(now.instant().plus(DEFAULT_VALIDITY))
              .build();
      SdkHttpFullRequest request =
          SdkHttpFullRequest.builder()
              .uri(new URI("https", host, "/", null))
              .appendRawQueryParameter(
                  "Action", (username.equals(ADMIN_USERNAME)) ? DB_CONNECT_ADMIN : DB_CONNECT)
              .method(SdkHttpMethod.GET)
              .build();

      return signer.presign(request, presignerParams).getUri().toString().replace("https://", "");
    } catch (URISyntaxException | SdkClientException e) {
      throw new RuntimeException(e);
    }
  }

  private static void validateUrl(String url) {
    String regex = "(^jdbc:postgresql:\\/\\/[a-zA-Z0-9_.-]+:[0-9]{0,9}\\/[a-zA-Z]*\\\\?.+$)1?";
    if (!url.matches(regex)) {
      throw new RuntimeException("Invalid JDBC url provided!");
    }
  }
}
