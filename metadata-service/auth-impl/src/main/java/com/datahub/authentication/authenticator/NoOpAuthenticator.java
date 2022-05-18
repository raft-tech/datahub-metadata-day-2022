package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.Authenticator;
import com.datahub.authentication.AuthenticatorContext;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.datahub.authentication.AuthenticationConstants.*;


/**
 * This Authenticator is used as a no-op to simply convert the X-DataHub-Actor header into a valid Authentication, or fall
 * back to resolving a system {@link Actor} by default.
 *
 * It exists to support deployments that do not have Metadata Service Authentication enabled.
 *
 * Notice that this authenticator should generally be avoided in production.
 */
@Slf4j
public class NoOpAuthenticator implements Authenticator {

  private String systemClientId;

  @Override
  public void init(@Nonnull final Map<String, Object> config) {
    Objects.requireNonNull(config, "Config parameter cannot be null");
    this.systemClientId = Objects.requireNonNull((String) config.get(SYSTEM_CLIENT_ID_CONFIG),
        String.format("Missing required config %s", SYSTEM_CLIENT_ID_CONFIG));
  }

  @Override
  public Authentication authenticate(@Nonnull AuthenticatorContext context) throws AuthenticationException {
    Objects.requireNonNull(context);
    String actorUrn = context.getRequestHeaders().get(LEGACY_X_DATAHUB_ACTOR_HEADER);

    // For backwards compatibility, support pulling actor context from the deprecated
    // X-DataHub-Actor header.
    if (actorUrn == null || "".equals(actorUrn)) {
      log.debug(String.format("Found no X-DataHub-Actor header provided with the request. Falling back to system creds %s", Constants.UNKNOWN_ACTOR));
      return new Authentication(
          new Actor(ActorType.USER, this.systemClientId), ""
      );
    }

    // If not provided, fallback to system caller identity.
    return new Authentication(
        // When authentication is disabled, assume everyone is a normal user.
        new Actor(ActorType.USER, getActorIdFromUrn(actorUrn)),
        "", // No Credentials provided.
        Collections.emptyMap()
    );
  }

  private String getActorIdFromUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr).getId();
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Failed to parse urn %s", urnStr));
    }
  }
}
