package com.datahub.authentication.token;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.authenticator.DataHubTokenAuthenticator;
import java.util.Map;
import org.testng.annotations.Test;

import static com.datahub.authentication.token.TokenClaims.*;
import static org.testng.Assert.*;


public class TokenServiceTest {

  private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";

  @Test
  public void testConstructor() {
    final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
    assertThrows(() -> new TokenService(null, null, null));
    assertThrows(() -> new TokenService(TEST_SIGNING_KEY, null, null));
    assertThrows(() -> new TokenService(TEST_SIGNING_KEY, "UNSUPPORTED_ALG", null));

    // Succeeds:
    new TokenService(TEST_SIGNING_KEY, "HS256");
    new TokenService(TEST_SIGNING_KEY, "HS256", null);
  }

  @Test
  public void testGenerateAccessTokenPersonalToken() throws Exception {
    TokenService tokenService = new TokenService(TEST_SIGNING_KEY, "HS256");
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = tokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.ONE);
    assertEquals(claims.getTokenType(), TokenType.PERSONAL);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertTrue(claims.getExpirationInMs() > System.currentTimeMillis());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 1);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "PERSONAL");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");

  }

  @Test
  public void testGenerateAccessTokenSessionToken() throws Exception {
    TokenService tokenService = new TokenService(TEST_SIGNING_KEY, "HS256");
    String token = tokenService.generateAccessToken(TokenType.SESSION, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Verify token claims
    TokenClaims claims = tokenService.validateAccessToken(token);

    assertEquals(claims.getTokenVersion(), TokenVersion.ONE);
    assertEquals(claims.getTokenType(), TokenType.SESSION);
    assertEquals(claims.getActorType(), ActorType.USER);
    assertEquals(claims.getActorId(), "datahub");
    assertTrue(claims.getExpirationInMs() > System.currentTimeMillis());

    Map<String, Object> claimsMap = claims.asMap();
    assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 1);
    assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "SESSION");
    assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
    assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
  }

  @Test
  public void testValidateAccessTokenFailsDueToExpiration() {
    TokenService tokenService = new TokenService(TEST_SIGNING_KEY, "HS256");
    // Generate token that expires immediately.
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"), 0L);
    assertNotNull(token);

    // Validation should fail.
    assertThrows(TokenExpiredException.class, () -> tokenService.validateAccessToken(token));
  }

  @Test
  public void testValidateAccessTokenFailsDueToManipulation() {
    TokenService tokenService = new TokenService(TEST_SIGNING_KEY, "HS256");
    String token = tokenService.generateAccessToken(TokenType.PERSONAL, new Actor(ActorType.USER, "datahub"));
    assertNotNull(token);

    // Change single character
    String changedToken = token.substring(1);

    // Validation should fail.
    assertThrows(TokenException.class, () -> tokenService.validateAccessToken(changedToken));
  }
}
