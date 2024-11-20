package com.oltpbenchmark.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public class TestIAMUtil {
  private AwsCredentialsProvider credentialsProvider;
  private DefaultAwsRegionProviderChain regionProvider;
  private static final String VALID_URL =
      "jdbc:postgresql://localhost:5432/postgres?sslmode=require&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true";
  private static final String VALID_ADMIN_USERNAME = "admin";

  @Before
  public void setup() {
    credentialsProvider = Mockito.mock(DefaultCredentialsProvider.class);
    AwsCredentials fakeCredentials =
        new AwsCredentials() {
          @Override
          public String accessKeyId() {
            return "ACCESS_KEY";
          }

          @Override
          public String secretAccessKey() {
            return "SECRET_KEY";
          }
        };
    Mockito.when(credentialsProvider.resolveCredentials()).thenReturn(fakeCredentials);

    regionProvider = Mockito.mock(DefaultAwsRegionProviderChain.class);
    Mockito.when(regionProvider.getRegion()).thenReturn(Region.US_EAST_2);
  }

  @Test
  public void testGenerateAuroraDsqlPasswordToken() {
    String token =
        IAMUtil.generateAuroraDsqlPasswordToken(
            VALID_URL, VALID_ADMIN_USERNAME, credentialsProvider, regionProvider);
    assertNotNull(token);
    assertTrue(token.contains("localhost/?"));
    assertTrue(token.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
    assertTrue(token.contains("X-Amz-Expires=3600"));
    assertTrue(token.contains("Action=DbConnectAdmin"));
    assertTrue(token.contains("X-Amz-Credential=ACCESS_KEY"));
    assertTrue(token.contains("X-Amz-Signature"));
  }

  @Test
  public void testGenerateAuroraDsqlPasswordTokenNonAdminUser() {
    String token =
        IAMUtil.generateAuroraDsqlPasswordToken(
            VALID_URL, "other", credentialsProvider, regionProvider);
    assertNotNull(token);
    assertTrue(token.contains("Action=DbConnect"));
  }

  @Test
  public void testGenerateAuroraDsqlPasswordTokenInvalidUrl() {
    assertThrows(
        RuntimeException.class,
        () ->
            IAMUtil.generateAuroraDsqlPasswordToken(
                "htp:/bad-url", VALID_ADMIN_USERNAME, credentialsProvider, regionProvider));
  }

  @Test
  public void testGenerateAuroraDsqlPasswordTokenAWSCredentialProviderSdkClientException() {
    Mockito.when(credentialsProvider.resolveCredentials()).thenThrow(SdkClientException.class);
    assertThrows(
        RuntimeException.class,
        () ->
            IAMUtil.generateAuroraDsqlPasswordToken(
                "htp:/bad-url", VALID_ADMIN_USERNAME, credentialsProvider, regionProvider));
  }
}
