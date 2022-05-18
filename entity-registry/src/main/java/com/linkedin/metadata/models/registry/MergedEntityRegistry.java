package com.linkedin.metadata.models.registry;

import com.linkedin.data.schema.compatibility.CompatibilityChecker;
import com.linkedin.data.schema.compatibility.CompatibilityOptions;
import com.linkedin.data.schema.compatibility.CompatibilityResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Combines results from two entity registries, where the second takes precedence
 */
@Slf4j
public class MergedEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;
  private final Map<String, EventSpec> eventNameToSpec;

  public MergedEntityRegistry(EntityRegistry baseEntityRegistry) {
    entityNameToSpec = baseEntityRegistry.getEntitySpecs() != null ? baseEntityRegistry.getEntitySpecs() : new HashMap<>();
    eventNameToSpec = baseEntityRegistry.getEventSpecs() != null ? baseEntityRegistry.getEventSpecs() : new HashMap<>();
  }

  private void validateEntitySpec(EntitySpec entitySpec, final ValidationResult validationResult) {
    if (entitySpec.getKeyAspectSpec() == null) {
      validationResult.setValid(false);
      validationResult.getValidationFailures().add(String.format("Key aspect is missing in entity {}", entitySpec.getName()));
    }
  }

  public MergedEntityRegistry apply(EntityRegistry patchEntityRegistry) throws EntityRegistryException {

    ValidationResult validationResult = validatePatch(patchEntityRegistry);
    if (!validationResult.isValid()) {
      throw new EntityRegistryException(String.format("Failed to validate new registry with %s", validationResult.validationFailures.stream().collect(
          Collectors.joining("\n"))));
    }

    // Merge Entity Specs
    for (Map.Entry<String, EntitySpec> e2Entry : patchEntityRegistry.getEntitySpecs().entrySet()) {
      if (entityNameToSpec.containsKey(e2Entry.getKey())) {
        EntitySpec mergeEntitySpec = mergeEntitySpecs(entityNameToSpec.get(e2Entry.getKey()), e2Entry.getValue());
        entityNameToSpec.put(e2Entry.getKey(), mergeEntitySpec);
      } else {
        // We are inserting a new entity into the registry
        entityNameToSpec.put(e2Entry.getKey(), e2Entry.getValue());
      }
    }

    // Merge Event Specs
    if (patchEntityRegistry.getEventSpecs().size() > 0) {
      eventNameToSpec.putAll(patchEntityRegistry.getEventSpecs());
    }
    //TODO: Validate that the entity registries don't have conflicts among each other
    return this;
  }

  private ValidationResult validatePatch(EntityRegistry patchEntityRegistry) {
    ValidationResult validationResult = new ValidationResult();
    for (Map.Entry<String, EntitySpec> e2Entry : patchEntityRegistry.getEntitySpecs().entrySet()) {
        checkMergeable(entityNameToSpec.getOrDefault(e2Entry.getKey(), null), e2Entry.getValue(), validationResult);
    }
    return validationResult;
  }

  private void checkMergeable(EntitySpec existingEntitySpec, EntitySpec newEntitySpec, final ValidationResult validationResult) {
    if (existingEntitySpec != null) {
      existingEntitySpec.getAspectSpecMap().entrySet().forEach(aspectSpecEntry -> {
        if (newEntitySpec.hasAspect(aspectSpecEntry.getKey())) {
          CompatibilityResult result = CompatibilityChecker.checkCompatibility(aspectSpecEntry.getValue().getPegasusSchema(), newEntitySpec.getAspectSpec(
              aspectSpecEntry.getKey()).getPegasusSchema(), new CompatibilityOptions());
          if (result.isError()) {
            log.error("{} schema is not compatible with previous schema due to {}", aspectSpecEntry.getKey(), result.getMessages());
            // we want to continue processing all aspects to collect all failures
            validationResult.setValid(false);
            validationResult.getValidationFailures().add(
                String.format("%s schema is not compatible with previous schema due to %s", aspectSpecEntry.getKey(), result.getMessages()));
          } else {
            log.info("{} schema is compatible with previous schema due to {}", aspectSpecEntry.getKey(), result.getMessages());
          }
        }
      });
    } else {
      validateEntitySpec(newEntitySpec, validationResult);
    }
  }


  private EntitySpec mergeEntitySpecs(EntitySpec existingEntitySpec, EntitySpec newEntitySpec) {
    Map<String, AspectSpec> aspectSpecMap = new HashMap<>(existingEntitySpec.getAspectSpecMap());
    aspectSpecMap.putAll(newEntitySpec.getAspectSpecMap());
    return new DefaultEntitySpec(aspectSpecMap.values(), existingEntitySpec.getEntityAnnotation(),
        existingEntitySpec.getSnapshotSchema(), existingEntitySpec.getAspectTyperefSchema());
  }

  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
    String lowercaseEntityName = entityName.toLowerCase();
    if (!entityNameToSpec.containsKey(lowercaseEntityName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find entity with name %s in EntityRegistry", entityName));
    }
    return entityNameToSpec.get(lowercaseEntityName);
  }

  @Nonnull
  @Override
  public EventSpec getEventSpec(@Nonnull String eventName) {
    String lowercaseEventSpec = eventName.toLowerCase();
    if (!eventNameToSpec.containsKey(lowercaseEventSpec)) {
      throw new IllegalArgumentException(
          String.format("Failed to find event with name %s in EntityRegistry", eventName));
    }
    return eventNameToSpec.get(lowercaseEventSpec);
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return entityNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return eventNameToSpec;
  }

  @Setter
  @Getter
  private class ValidationResult {
    boolean valid = true;
    List<String> validationFailures = new ArrayList<>();
  }
}
