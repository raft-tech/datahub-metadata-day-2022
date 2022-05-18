package com.linkedin.metadata.kafka.hook.event;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Deprecation;
import com.linkedin.common.FabricType;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.differ.AspectDifferRegistry;
import com.linkedin.metadata.timeline.differ.DeprecationDiffer;
import com.linkedin.metadata.timeline.differ.EntityKeyDiffer;
import com.linkedin.metadata.timeline.differ.GlobalTagsDiffer;
import com.linkedin.metadata.timeline.differ.GlossaryTermsDiffer;
import com.linkedin.metadata.timeline.differ.OwnershipDiffer;
import com.linkedin.metadata.timeline.differ.SingleDomainDiffer;
import com.linkedin.metadata.timeline.differ.StatusDiffer;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.PlatformEvent;
import com.linkedin.mxe.PlatformEventHeader;
import com.linkedin.platform.event.v1.EntityChangeEvent;
import com.linkedin.platform.event.v1.Parameters;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;

import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


/**
 * Tests the {@link EntityChangeEventGeneratorHook}.
 *
 * TODO: Include Schema Field Tests, description update tests.
 */
public class EntityChangeEventGeneratorHookTest {

  private static final String TEST_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,SampleDataset,PROD)";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  private RestliEntityClient _mockClient;
  private EntityChangeEventGeneratorHook _entityChangeEventHook;

  @BeforeMethod
  public void setupTest() {
    AspectDifferRegistry differRegistry = createAspectDifferRegistry();
    Authentication mockAuthentication = Mockito.mock(Authentication.class);
    _mockClient = Mockito.mock(RestliEntityClient.class);
    _entityChangeEventHook = new EntityChangeEventGeneratorHook(
        differRegistry,
        _mockClient,
        mockAuthentication,
        createMockEntityRegistry());
  }

  @Test
  public void testInvokeEntityAddTagChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlobalTags newTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn("Test");
    final long eventTime = 123L;
    newTags.setTags(new TagAssociationArray(
        ImmutableList.of(new TagAssociation()
            .setTag(newTagUrn)
        )
    ));
    event.setAspect(GenericRecordUtils.serializeAspect(newTags));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.TAG,
        ChangeOperation.ADD,
        newTagUrn.toString(),
        ImmutableMap.of(
            "tagUrn", newTagUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityRemoveTagChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOBAL_TAGS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlobalTags existingTags = new GlobalTags();
    final TagUrn newTagUrn = new TagUrn("Test");
    final long eventTime = 123L;
    existingTags.setTags(new TagAssociationArray(
        ImmutableList.of(new TagAssociation()
            .setTag(newTagUrn)
        )
    ));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(existingTags));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.TAG,
        ChangeOperation.REMOVE,
        newTagUrn.toString(),
        ImmutableMap.of(
            "tagUrn", newTagUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityAddTermChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOSSARY_TERMS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlossaryTerms newTerms = new GlossaryTerms();
    final GlossaryTermUrn glossaryTermUrn = new GlossaryTermUrn("TestTerm");
    final long eventTime = 123L;
    newTerms.setTerms(new GlossaryTermAssociationArray(
        ImmutableList.of(new GlossaryTermAssociation()
            .setUrn(glossaryTermUrn)
        )
    ));
    final GlossaryTerms previousTerms = new GlossaryTerms();
    previousTerms.setTerms(new GlossaryTermAssociationArray());
    event.setAspect(GenericRecordUtils.serializeAspect(newTerms));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousTerms));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.GLOSSARY_TERM,
        ChangeOperation.ADD,
        glossaryTermUrn.toString(),
        ImmutableMap.of(
            "termUrn", glossaryTermUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityRemoveTermChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(GLOSSARY_TERMS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final GlossaryTerms newTerms = new GlossaryTerms();
    newTerms.setTerms(new GlossaryTermAssociationArray());
    final GlossaryTerms previousTerms = new GlossaryTerms();
    final GlossaryTermUrn glossaryTermUrn = new GlossaryTermUrn("TestTerm");
    final long eventTime = 123L;
    previousTerms.setTerms(new GlossaryTermAssociationArray(
        ImmutableList.of(new GlossaryTermAssociation()
            .setUrn(glossaryTermUrn)
        )
    ));
    event.setAspect(GenericRecordUtils.serializeAspect(newTerms));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousTerms));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.GLOSSARY_TERM,
        ChangeOperation.REMOVE,
        glossaryTermUrn.toString(),
        ImmutableMap.of(
            "termUrn", glossaryTermUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntitySetDomain() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DOMAINS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final Domains newDomains = new Domains();
    final Urn domainUrn = Urn.createFromString("urn:li:domain:test");
    final long eventTime = 123L;
    newDomains.setDomains(new UrnArray(
        ImmutableList.of(domainUrn)
    ));
    event.setAspect(GenericRecordUtils.serializeAspect(newDomains));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.DOMAIN,
        ChangeOperation.ADD,
        domainUrn.toString(),
        ImmutableMap.of(
            "domainUrn", domainUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityUnsetDomain() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DOMAINS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final Domains previousDomains = new Domains();
    final Urn domainUrn = Urn.createFromString("urn:li:domain:test");
    final long eventTime = 123L;
    previousDomains.setDomains(new UrnArray(
        ImmutableList.of(domainUrn)
    ));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(previousDomains));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.DOMAIN,
        ChangeOperation.REMOVE,
        domainUrn.toString(),
        ImmutableMap.of(
            "domainUrn", domainUrn.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityOwnerChange() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(OWNERSHIP_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);
    final Ownership newOwners = new Ownership();
    final Urn ownerUrn1 = Urn.createFromString("urn:li:corpuser:test1");
    final Urn ownerUrn2 = Urn.createFromString("urn:li:corpuser:test2");
    final long eventTime = 123L;
    newOwners.setOwners(new OwnerArray(
        ImmutableList.of(
            new Owner().setOwner(ownerUrn1).setType(OwnershipType.TECHNICAL_OWNER),
            new Owner().setOwner(ownerUrn2).setType(OwnershipType.BUSINESS_OWNER)
        )
    ));
    final Ownership prevOwners = new Ownership();
    prevOwners.setOwners(new OwnerArray());
    event.setAspect(GenericRecordUtils.serializeAspect(newOwners));
    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(prevOwners));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent1 = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.OWNER,
        ChangeOperation.ADD,
        ownerUrn1.toString(),
        ImmutableMap.of(
            "ownerUrn", ownerUrn1.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );
    verifyProducePlatformEvent(_mockClient, platformEvent1, false);

    PlatformEvent platformEvent2 = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.OWNER,
        ChangeOperation.ADD,
        ownerUrn2.toString(),
        ImmutableMap.of(
            "ownerUrn", ownerUrn2.toString()
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );
    verifyProducePlatformEvent(_mockClient, platformEvent2, true);
  }

  @Test
  public void testInvokeEntityTermDeprecation() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DEPRECATION_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    Deprecation newDeprecation = new Deprecation();
    newDeprecation.setDeprecated(true);
    newDeprecation.setNote("Test Note");
    newDeprecation.setActor(Urn.createFromString(TEST_ACTOR_URN));
    final long eventTime = 123L;

    event.setAspect(GenericRecordUtils.serializeAspect(newDeprecation));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.DEPRECATION,
        ChangeOperation.MODIFY,
        null,
        ImmutableMap.of(
            "status", "DEPRECATED"
        ),
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityCreate() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATASET_KEY_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    DatasetKey newDatasetKey = new DatasetKey();
    newDatasetKey.setName("TestName");
    newDatasetKey.setOrigin(FabricType.PROD);
    newDatasetKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"));
    final long eventTime = 123L;

    event.setAspect(GenericRecordUtils.serializeAspect(newDatasetKey));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.LIFECYCLE,
        ChangeOperation.CREATE,
        null,
        null,
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntityHardDelete() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATASET_KEY_ASPECT_NAME);
    event.setChangeType(ChangeType.DELETE);

    DatasetKey newDatasetKey = new DatasetKey();
    newDatasetKey.setName("TestName");
    newDatasetKey.setOrigin(FabricType.PROD);
    newDatasetKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"));
    final long eventTime = 123L;

    event.setPreviousAspectValue(GenericRecordUtils.serializeAspect(newDatasetKey));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.LIFECYCLE,
        ChangeOperation.HARD_DELETE,
        null,
        null,
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeEntitySoftDelete() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(STATUS_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    Status newStatus = new Status();
    newStatus.setRemoved(true);
    final long eventTime = 123L;

    event.setAspect(GenericRecordUtils.serializeAspect(newStatus));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(eventTime));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );

    // Create Platform Event
    PlatformEvent platformEvent = createChangeEvent(
        DATASET_ENTITY_NAME,
        Urn.createFromString(TEST_DATASET_URN),
        ChangeCategory.LIFECYCLE,
        ChangeOperation.SOFT_DELETE,
        null,
        null,
        Urn.createFromString(TEST_ACTOR_URN),
        eventTime
    );

    verifyProducePlatformEvent(_mockClient, platformEvent);
  }

  @Test
  public void testInvokeIneligibleAspect() throws Exception {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setAspectName(DATAHUB_POLICY_INFO_ASPECT_NAME);
    event.setChangeType(ChangeType.UPSERT);

    DatasetProperties props = new DatasetProperties();
    props.setName("Test name");

    event.setAspect(GenericRecordUtils.serializeAspect(props));
    event.setEntityUrn(Urn.createFromString(TEST_DATASET_URN));
    event.setEntityType(DATASET_ENTITY_NAME);
    event.setCreated(new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(123L));

    // No previous tags aspect.
    _entityChangeEventHook.invoke(
        event
    );
    // Verify 0 interactions
    Mockito.verifyNoMoreInteractions(_mockClient);
  }

  private PlatformEvent createChangeEvent(
      String entityType,
      Urn entityUrn,
      ChangeCategory category,
      ChangeOperation operation,
      String modifier,
      Map<String, Object> parameters,
      Urn actor,
      long timestamp) throws Exception {
    final EntityChangeEvent changeEvent = new EntityChangeEvent();
    changeEvent.setEntityType(entityType);
    changeEvent.setEntityUrn(entityUrn);
    changeEvent.setCategory(category.name());
    changeEvent.setOperation(operation.name());
    if (modifier != null) {
      changeEvent.setModifier(modifier);
    }
    changeEvent.setAuditStamp(new AuditStamp()
      .setActor(actor)
      .setTime(timestamp)
    );
    changeEvent.setVersion(0);
    if (parameters != null) {
      changeEvent.setParameters(new Parameters(new DataMap(parameters)));
    }
    final PlatformEvent platformEvent = new PlatformEvent();
    platformEvent.setName(CHANGE_EVENT_PLATFORM_EVENT_NAME);
    platformEvent.setHeader(new PlatformEventHeader().setTimestampMillis(timestamp));
    platformEvent.setPayload(GenericRecordUtils.serializePayload(
        changeEvent
    ));
    return platformEvent;
  }

  private AspectDifferRegistry createAspectDifferRegistry() {
    final AspectDifferRegistry registry = new AspectDifferRegistry();
    registry.register(GLOBAL_TAGS_ASPECT_NAME, new GlobalTagsDiffer());
    registry.register(GLOSSARY_TERMS_ASPECT_NAME, new GlossaryTermsDiffer());
    registry.register(DOMAINS_ASPECT_NAME, new SingleDomainDiffer());
    registry.register(OWNERSHIP_ASPECT_NAME, new OwnershipDiffer());
    registry.register(STATUS_ASPECT_NAME, new StatusDiffer());
    registry.register(DEPRECATION_ASPECT_NAME, new DeprecationDiffer());

    // TODO Add Dataset Schema Field related differs.

    // Entity Lifecycle Differs
    registry.register(DATASET_KEY_ASPECT_NAME, new EntityKeyDiffer<>());
    return registry;
  }

  private EntityRegistry createMockEntityRegistry() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    // Build Dataset Entity Spec
    EntitySpec datasetSpec = Mockito.mock(EntitySpec.class);

    AspectSpec mockTags = createMockAspectSpec(GlobalTags.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(GLOBAL_TAGS_ASPECT_NAME))).thenReturn(mockTags);

    AspectSpec mockTerms = createMockAspectSpec(GlossaryTerms.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(GLOSSARY_TERMS_ASPECT_NAME))).thenReturn(mockTerms);

    AspectSpec mockOwners = createMockAspectSpec(Ownership.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(OWNERSHIP_ASPECT_NAME))).thenReturn(mockOwners);

    AspectSpec mockStatus = createMockAspectSpec(Status.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(STATUS_ASPECT_NAME))).thenReturn(mockStatus);

    AspectSpec mockDomains = createMockAspectSpec(Domains.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(DOMAINS_ASPECT_NAME))).thenReturn(mockDomains);

    AspectSpec mockDeprecation = createMockAspectSpec(Deprecation.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(DEPRECATION_ASPECT_NAME))).thenReturn(mockDeprecation);

    AspectSpec mockDatasetKey = createMockAspectSpec(DatasetKey.class);
    Mockito.when(datasetSpec.getAspectSpec(Mockito.eq(DATASET_KEY_ASPECT_NAME))).thenReturn(mockDatasetKey);

    Mockito.when(registry.getEntitySpec(Mockito.eq(DATASET_ENTITY_NAME)))
        .thenReturn(datasetSpec);
    return registry;
  }

  private void verifyProducePlatformEvent(EntityClient mockClient, PlatformEvent platformEvent) throws Exception {
    verifyProducePlatformEvent(mockClient, platformEvent, true);
  }

  private void verifyProducePlatformEvent(EntityClient mockClient, PlatformEvent platformEvent, boolean noMoreInteractions) throws Exception {
    // Verify event has been emitted.
    Mockito.verify(mockClient, Mockito.times(1))
        .producePlatformEvent(
            Mockito.eq(CHANGE_EVENT_PLATFORM_EVENT_NAME),
            Mockito.anyString(),
            Mockito.eq(platformEvent),
            Mockito.any(Authentication.class)
        );

    if (noMoreInteractions) {
      Mockito.verifyNoMoreInteractions(_mockClient);
    }
  }

  private <T extends RecordTemplate> AspectSpec createMockAspectSpec(Class<T> clazz) {
    AspectSpec mockSpec = Mockito.mock(AspectSpec.class);
    Mockito.when(mockSpec.getDataTemplateClass()).thenReturn((Class<RecordTemplate>) clazz);
    return mockSpec;
  }
}