package com.linkedin.datahub.graphql.types.assertion;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AssertionTypeTest {

  private static final String TEST_ASSERTION_URN = "urn:li:assertion:guid-1";
  private static final AssertionKey TEST_ASSERTION_KEY = new AssertionKey()
      .setAssertionId("guid-1");
  private static final AssertionInfo TEST_ASSERTION_INFO = new AssertionInfo()
      .setType(AssertionType.DATASET)
      .setDatasetAssertion(null, SetMode.IGNORE_NULL)
      .setCustomProperties(new StringMap());
  private static final DataPlatformInstance TEST_DATA_PLATFORM_INSTANCE = new DataPlatformInstance()
      .setPlatform(new DataPlatformUrn("snowflake"))
      .setInstance(null, SetMode.IGNORE_NULL);

  private static final String TEST_ASSERTION_URN_2 = "urn:li:assertion:guid-2";


  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn assertionUrn1 = Urn.createFromString(TEST_ASSERTION_URN);
    Urn assertionUrn2 = Urn.createFromString(TEST_ASSERTION_URN_2);

    Map<String, EnvelopedAspect> assertion1Aspects = new HashMap<>();
    assertion1Aspects.put(
        Constants.ASSERTION_KEY_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_KEY.data()))
    );
    assertion1Aspects.put(
        Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_DATA_PLATFORM_INSTANCE.data()))
    );
    assertion1Aspects.put(
        Constants.ASSERTION_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_ASSERTION_INFO.data()))
    );
    Mockito.when(client.batchGetV2(
        Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
        Mockito.eq(new HashSet<>(ImmutableSet.of(assertionUrn1, assertionUrn2))),
        Mockito.eq(com.linkedin.datahub.graphql.types.assertion.AssertionType.ASPECTS_TO_FETCH),
        Mockito.any(Authentication.class)))
        .thenReturn(ImmutableMap.of(
            assertionUrn1,
            new EntityResponse()
                .setEntityName(Constants.ASSERTION_ENTITY_NAME)
                .setUrn(assertionUrn1)
                .setAspects(new EnvelopedAspectMap(assertion1Aspects))));

    com.linkedin.datahub.graphql.types.assertion.AssertionType type = new com.linkedin.datahub.graphql.types.assertion.AssertionType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    List<DataFetcherResult<Assertion>> result = type.batchLoad(ImmutableList.of(TEST_ASSERTION_URN, TEST_ASSERTION_URN_2), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1)).batchGetV2(
        Mockito.eq(Constants.ASSERTION_ENTITY_NAME),
        Mockito.eq(ImmutableSet.of(assertionUrn1, assertionUrn2)),
        Mockito.eq(com.linkedin.datahub.graphql.types.assertion.AssertionType.ASPECTS_TO_FETCH),
        Mockito.any(Authentication.class)
    );

    assertEquals(result.size(), 2);

    Assertion assertion = result.get(0).getData();
    assertEquals(assertion.getUrn(), TEST_ASSERTION_URN);
    assertEquals(assertion.getType(), EntityType.ASSERTION);
    assertEquals(assertion.getInfo().getType().toString(), AssertionType.DATASET.toString());
    assertEquals(assertion.getInfo().getDatasetAssertion(), null);
    assertEquals(assertion.getPlatform().getUrn(), "urn:li:dataPlatform:snowflake");

    // Assert second element is null.
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class).when(mockClient).batchGetV2(
        Mockito.anyString(),
        Mockito.anySet(),
        Mockito.anySet(),
        Mockito.any(Authentication.class));
    com.linkedin.datahub.graphql.types.assertion.AssertionType type = new com.linkedin.datahub.graphql.types.assertion.AssertionType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(RuntimeException.class, () -> type.batchLoad(ImmutableList.of(TEST_ASSERTION_URN, TEST_ASSERTION_URN_2),
        context));
  }
}