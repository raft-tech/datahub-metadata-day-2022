package com.linkedin.datahub.graphql.resolvers;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AuthenticatedUser;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.PlatformPrivileges;
import com.linkedin.datahub.graphql.types.corpuser.mappers.CorpUserMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils.*;
import static com.linkedin.metadata.Constants.*;


/**
 * GraphQL resolver responsible for resolving information about the currently
 * logged in User, including
 *
 *    1. User profile information
 *    2. User privilege information, i.e. which features to display in the UI.
 *
 */
public class MeResolver implements DataFetcher<CompletableFuture<AuthenticatedUser>> {

  private final EntityClient _entityClient;

  public MeResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<AuthenticatedUser> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    return CompletableFuture.supplyAsync(() -> {
      try {
        // 1. Get currently logged in user profile.
        final Urn userUrn = Urn.createFromString(context.getActorUrn());
        final EntityResponse gmsUser = _entityClient.batchGetV2(CORP_USER_ENTITY_NAME,
                Collections.singleton(userUrn), null, context.getAuthentication()).get(userUrn);
        final CorpUser corpUser = CorpUserMapper.map(gmsUser);

        // 2. Get platform privileges
        final PlatformPrivileges platformPrivileges = new PlatformPrivileges();
        platformPrivileges.setViewAnalytics(canViewAnalytics(context));
        platformPrivileges.setManagePolicies(canManagePolicies(context));
        platformPrivileges.setManageIdentities(canManageUsersGroups(context));
        platformPrivileges.setGeneratePersonalAccessTokens(canGeneratePersonalAccessToken(context));
        platformPrivileges.setManageDomains(canManageDomains(context));
        platformPrivileges.setManageIngestion(canManageIngestion(context));
        platformPrivileges.setManageSecrets(canManageSecrets(context));

        // Construct and return authenticated user object.
        final AuthenticatedUser authUser = new AuthenticatedUser();
        authUser.setCorpUser(corpUser);
        authUser.setPlatformPrivileges(platformPrivileges);
        return authUser;
      } catch (URISyntaxException | RemoteInvocationException e) {
        throw new RuntimeException("Failed to fetch authenticated user!", e);
      }
    });
  }

  /**
   * Returns true if the authenticated user has privileges to view analytics.
   */
  private boolean canViewAnalytics(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), PoliciesConfig.VIEW_ANALYTICS_PRIVILEGE);
  }

  /**
   * Returns true if the authenticated user has privileges to manage policies analytics.
   */
  private boolean canManagePolicies(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), PoliciesConfig.MANAGE_POLICIES_PRIVILEGE);
  }

  /**
   * Returns true if the authenticated user has privileges to manage users & groups.
   */
  private boolean canManageUsersGroups(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), PoliciesConfig.MANAGE_USERS_AND_GROUPS_PRIVILEGE);
  }

  /**
   * Returns true if the authenticated user has privileges to generate personal access tokens
   */
  private boolean canGeneratePersonalAccessToken(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), PoliciesConfig.GENERATE_PERSONAL_ACCESS_TOKENS_PRIVILEGE);
  }


  /**
   * Returns true if the authenticated user has privileges to manage domains
   */
  private boolean canManageDomains(final QueryContext context) {
    return isAuthorized(context.getAuthorizer(), context.getActorUrn(), PoliciesConfig.MANAGE_DOMAINS_PRIVILEGE);
  }

  /**
   * Returns true if the the provided actor is authorized for a particular privilege, false otherwise.
   */
  private boolean isAuthorized(final Authorizer authorizer, String actor, PoliciesConfig.Privilege privilege) {
    final AuthorizationRequest request = new AuthorizationRequest(actor, privilege.getType(), Optional.empty());
    final AuthorizationResult result = authorizer.authorize(request);
    return AuthorizationResult.Type.ALLOW.equals(result.getType());
  }
}
