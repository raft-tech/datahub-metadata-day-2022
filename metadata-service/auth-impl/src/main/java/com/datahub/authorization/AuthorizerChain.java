package com.datahub.authorization;

import com.linkedin.common.urn.Urn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * A configurable chain of {@link Authorizer}s executed in series to attempt to authenticate an inbound request.
 *
 * Individual {@link Authorizer}s are registered with the chain using {@link #register(Authorizer)}.
 * The chain can be executed by invoking {@link #authorize(AuthorizationRequest)}.
 */
@Slf4j
public class AuthorizerChain implements Authorizer {

  private final List<Authorizer> authorizers;

  public AuthorizerChain(final List<Authorizer> authorizers) {
    this.authorizers = Objects.requireNonNull(authorizers);
  }

  @Override
  public void init(@Nonnull Map<String, Object> authorizerConfig) {
    // pass.
  }

  /**
   * Executes a set of {@link Authorizer}s and returns the first successful authentication result.
   *
   * Returns an instance of {@link AuthorizationResult}.
   */
  @Nullable
  public AuthorizationResult authorize(@Nonnull final AuthorizationRequest request) {
    Objects.requireNonNull(request);
    for (final Authorizer authorizer : this.authorizers) {
      try {
        log.debug("Executing Authorizer with class name {}", authorizer.getClass().getCanonicalName());
        AuthorizationResult result = authorizer.authorize(request);
        if (AuthorizationResult.Type.ALLOW.equals(result.type)) {
          // Authorization was successful - Short circuit
          return result;
        } else {
          log.debug("Received DENY result from Authorizer with class name {}. message: {}",
              authorizer.getClass().getCanonicalName(), result.getMessage());
        }
      } catch (Exception e) {
        log.error("Caught exception while attempting to authorize request using Authorizer {}. Skipping authorizer.",
            authorizer.getClass().getCanonicalName(), e);
      }
    }
    // Return failed Authorization result.
    return new AuthorizationResult(request, AuthorizationResult.Type.DENY, null);
  }

  @Override
  public AuthorizedActors authorizedActors(String privilege, Optional<ResourceSpec> resourceSpec) {
    if (this.authorizers.isEmpty()) {
      return null;
    }

    AuthorizedActors finalAuthorizedActors = this.authorizers.get(0).authorizedActors(privilege, resourceSpec);
    for (int i = 1; i < this.authorizers.size(); i++) {
      finalAuthorizedActors = mergeAuthorizedActors(finalAuthorizedActors,
          this.authorizers.get(i).authorizedActors(privilege, resourceSpec));
    }
    return finalAuthorizedActors;
  }

  private AuthorizedActors mergeAuthorizedActors(@Nullable AuthorizedActors original,
      @Nullable AuthorizedActors other) {
    if (original == null) {
      return other;
    }
    if (other == null) {
      return original;
    }

    boolean isAllUsers = original.isAllUsers() || other.isAllUsers();
    List<Urn> mergedUsers;
    if (isAllUsers) {
      // If enabled for all users, no need to check users
      mergedUsers = Collections.emptyList();
    } else {
      Set<Urn> users = new HashSet<>(original.getUsers());
      users.addAll(other.getUsers());
      mergedUsers = new ArrayList<>(users);
    }

    boolean isAllGroups = original.isAllGroups() || other.isAllGroups();
    List<Urn> mergedGroups;
    if (isAllGroups) {
      // If enabled for all users, no need to check users
      mergedGroups = Collections.emptyList();
    } else {
      Set<Urn> groups = new HashSet<>(original.getGroups());
      groups.addAll(other.getGroups());
      mergedGroups = new ArrayList<>(groups);
    }

    return AuthorizedActors.builder()
        .allUsers(original.isAllUsers() || other.isAllUsers())
        .allGroups(original.isAllGroups() || other.isAllGroups())
        .users(mergedUsers)
        .groups(mergedGroups)
        .build();
  }

  /**
   * Returns an instance of {@link DataHubAuthorizer} if it is present in the Authentication chain,
   * or null if it cannot be found.
   */
  public DataHubAuthorizer getDefaultAuthorizer() {
    return (DataHubAuthorizer) this.authorizers.stream()
        .filter(authorizer -> authorizer instanceof DataHubAuthorizer)
        .findFirst()
        .orElse(null);
  }
}