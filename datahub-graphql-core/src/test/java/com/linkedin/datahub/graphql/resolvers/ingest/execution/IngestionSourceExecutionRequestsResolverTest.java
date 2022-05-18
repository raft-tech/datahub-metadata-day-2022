package com.linkedin.datahub.graphql.resolvers.ingest.execution;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourceExecutionRequests;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.ingest.IngestTestUtils.*;
import static org.testng.Assert.*;


public class IngestionSourceExecutionRequestsResolverTest {

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    // Mock filter response
    Mockito.when(mockClient.filter(
        Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
        Mockito.any(Filter.class),
        Mockito.any(SortCriterion.class),
        Mockito.eq(0),
        Mockito.eq(10),
        Mockito.any(Authentication.class)))
        .thenReturn(new SearchResult()
          .setFrom(0)
          .setPageSize(10)
          .setNumEntities(1)
          .setEntities(new SearchEntityArray(ImmutableList.of(
              new SearchEntity().setEntity(TEST_EXECUTION_REQUEST_URN))))
        );

    // Mock batch get response
    ExecutionRequestInput returnedInput = getTestExecutionRequestInput();
    ExecutionRequestResult returnedResult = getTestExecutionRequestResult();

    Mockito.when(mockClient.batchGetV2(
        Mockito.eq(Constants.EXECUTION_REQUEST_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_EXECUTION_REQUEST_URN))),
        Mockito.eq(ImmutableSet.of(
            Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
            Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME)),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(TEST_EXECUTION_REQUEST_URN,
            new EntityResponse().setEntityName(Constants.EXECUTION_REQUEST_ENTITY_NAME)
                .setUrn(TEST_EXECUTION_REQUEST_URN)
                .setAspects(new EnvelopedAspectMap(ImmutableMap.of(
                    Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(returnedInput.data()))
                        .setCreated(new AuditStamp()
                          .setTime(0L)
                          .setActor(Urn.createFromString("urn:li:corpuser:test"))),
                    Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME,
                    new EnvelopedAspect().setValue(new Aspect(returnedResult.data()))
                        .setCreated(new AuditStamp()
                          .setTime(0L)
                          .setActor(Urn.createFromString("urn:li:corpuser:test")))
                )))));

    IngestionSourceExecutionRequestsResolver resolver = new IngestionSourceExecutionRequestsResolver(mockClient);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    // Data Assertions
    IngestionSourceExecutionRequests executionRequests = resolver.get(mockEnv).get();
    assertEquals((int) executionRequests.getStart(), 0);
    assertEquals((int) executionRequests.getCount(), 10);
    assertEquals((int) executionRequests.getTotal(), 1);
    verifyTestExecutionRequest(executionRequests.getExecutionRequests().get(0), returnedInput, returnedResult);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    IngestionSourceExecutionRequestsResolver resolver = new IngestionSourceExecutionRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).batchGetV2(
        Mockito.any(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    Mockito.verify(mockClient, Mockito.times(0)).list(
        Mockito.any(),
        Mockito.anyMap(),
        Mockito.anyInt(),
        Mockito.anyInt(),
        Mockito.any(Authentication.class));
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).batchGetV2(
        Mockito.any(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    IngestionSourceExecutionRequestsResolver resolver = new IngestionSourceExecutionRequestsResolver(mockClient);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("start"))).thenReturn(0);
    Mockito.when(mockEnv.getArgument(Mockito.eq("count"))).thenReturn(10);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    IngestionSource parentSource = new IngestionSource();
    parentSource.setUrn(TEST_INGESTION_SOURCE_URN.toString());
    Mockito.when(mockEnv.getSource()).thenReturn(parentSource);

    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv).join());
  }
}
