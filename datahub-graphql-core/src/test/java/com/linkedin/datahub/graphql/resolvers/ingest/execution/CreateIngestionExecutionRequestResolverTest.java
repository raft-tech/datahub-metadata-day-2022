package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateIngestionExecutionRequestInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;


public class CreateIngestionExecutionRequestResolverTest {

  private static final CreateIngestionExecutionRequestInput TEST_INPUT = new CreateIngestionExecutionRequestInput(
      TEST_INGESTION_SOURCE_URN.toString()
  );

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.INGESTION_SOURCE_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_INGESTION_SOURCE_URN))),
        Mockito.eq(ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME)),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(TEST_INGESTION_SOURCE_URN,
            new EntityResponse().setEntityName(Constants.INGESTION_SOURCE_ENTITY_NAME)
                .setUrn(TEST_INGESTION_SOURCE_URN)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    Constants.INGESTION_INFO_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(getTestIngestionSourceInfo().data()))
                )))));
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateIngestionExecutionRequestResolver resolver = new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    // Not ideal to match against "any", but we don't know the auto-generated execution request id
    Mockito.verify(mockClient, Mockito.times(1)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateIngestionExecutionRequestResolver resolver = new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).ingestProposal(
        Mockito.any(),
        Mockito.any(Authentication.class));
    IngestionConfiguration ingestionConfiguration = new IngestionConfiguration();
    ingestionConfiguration.setDefaultCliVersion("default");
    CreateIngestionExecutionRequestResolver resolver = new CreateIngestionExecutionRequestResolver(mockClient, ingestionConfiguration);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}

