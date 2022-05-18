package auth.sso.oidc;

import auth.sso.SsoProvider;
import auth.sso.oidc.custom.CustomOidcClient;
import com.google.common.collect.ImmutableMap;
import org.pac4j.core.client.Client;
import org.pac4j.core.http.callback.PathParameterCallbackUrlResolver;
import org.pac4j.oidc.config.OidcConfiguration;
import org.pac4j.oidc.credentials.OidcCredentials;
import org.pac4j.oidc.profile.OidcProfile;


/**
 * Implementation of {@link SsoProvider} supporting the OIDC protocol.
 *
 * This class is a thin wrapper over a Pac4J {@link Client} object and all DataHub-specific OIDC related
 * configuration options, which reside in an instance of {@link OidcConfigs}.
 *
 * It is responsible for initializing this client from a configuration object ({@link OidcConfigs}. Note that
 * this class is not related to the logic performed when an IdP performs a callback to DataHub.
 */
public class OidcProvider implements SsoProvider<OidcConfigs> {

  private static final String OIDC_CLIENT_NAME = "oidc";

  private final OidcConfigs _oidcConfigs;
  private final Client<OidcCredentials, OidcProfile> _oidcClient; // Used primarily for redirecting to IdP.

  public OidcProvider(final OidcConfigs configs) {
    _oidcConfigs = configs;
    _oidcClient = createPac4jClient();
  }

  @Override
  public Client<OidcCredentials, OidcProfile> client() {
    return _oidcClient;
  }

  @Override
  public OidcConfigs configs() {
    return _oidcConfigs;
  }

  @Override
  public SsoProtocol protocol() {
    return SsoProtocol.OIDC;
  }

  private Client<OidcCredentials, OidcProfile> createPac4jClient() {
    final OidcConfiguration oidcConfiguration = new OidcConfiguration();
    oidcConfiguration.setClientId(_oidcConfigs.getClientId());
    oidcConfiguration.setSecret(_oidcConfigs.getClientSecret());
    oidcConfiguration.setDiscoveryURI(_oidcConfigs.getDiscoveryUri());
    oidcConfiguration.setClientAuthenticationMethodAsString(_oidcConfigs.getClientAuthenticationMethod());
    oidcConfiguration.setScope(_oidcConfigs.getScope());
    _oidcConfigs.getResponseType().ifPresent(oidcConfiguration::setResponseType);
    _oidcConfigs.getResponseMode().ifPresent(oidcConfiguration::setResponseMode);
    _oidcConfigs.getUseNonce().ifPresent(oidcConfiguration::setUseNonce);
    _oidcConfigs.getCustomParamResource()
        .ifPresent(value -> oidcConfiguration.setCustomParams(ImmutableMap.of("resource", value)));

    final CustomOidcClient oidcClient = new CustomOidcClient(oidcConfiguration);
    oidcClient.setName(OIDC_CLIENT_NAME);
    oidcClient.setCallbackUrl(_oidcConfigs.getAuthBaseUrl() + _oidcConfigs.getAuthBaseCallbackPath());
    oidcClient.setCallbackUrlResolver(new PathParameterCallbackUrlResolver());
    return oidcClient;
  }
}
