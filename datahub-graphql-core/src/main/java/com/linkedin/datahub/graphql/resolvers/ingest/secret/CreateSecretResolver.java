package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateSecretInput;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionAuthUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubSecretKey;
import com.linkedin.metadata.secret.SecretService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.secret.DataHubSecretValue;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Creates an encrypted DataHub secret. Uses AES symmetric encryption / decryption. Requires the MANAGE_SECRETS privilege.
 */
public class CreateSecretResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _entityClient;
  private final SecretService _secretService;

  public CreateSecretResolver(
      final EntityClient entityClient,
      final SecretService secretService
  ) {
    _entityClient = entityClient;
    _secretService = secretService;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (IngestionAuthUtils.canManageSecrets(context)) {

        final CreateSecretInput input = bindArgument(environment.getArgument("input"), CreateSecretInput.class);

        final MetadataChangeProposal proposal = new MetadataChangeProposal();

        // Create the Ingestion source key --> use the display name as a unique id to ensure it's not duplicated.
        final DataHubSecretKey key = new DataHubSecretKey();
        key.setId(input.getName());
        proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

        // Create the secret value.
        final DataHubSecretValue value = new DataHubSecretValue();
        value.setName(input.getName());
        value.setValue(_secretService.encrypt(input.getValue()));
        value.setDescription(input.getDescription(), SetMode.IGNORE_NULL);

        proposal.setEntityType(Constants.SECRETS_ENTITY_NAME);
        proposal.setAspectName(Constants.SECRET_VALUE_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(value));
        proposal.setChangeType(ChangeType.UPSERT);

        System.out.println(String.format("About to ingest %s", proposal));

        try {
          return _entityClient.ingestProposal(proposal, context.getAuthentication());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Failed to create new secret with name %s", input.getName()), e);
        }
      }
      throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }
}
