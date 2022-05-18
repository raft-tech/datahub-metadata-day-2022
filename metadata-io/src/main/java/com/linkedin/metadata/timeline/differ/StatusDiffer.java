package com.linkedin.metadata.timeline.differ;

import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Differ responsible for determining whether an entity has been soft-deleted or soft-created.
 */
public class StatusDiffer implements AspectDiffer<Status> {
  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    // TODO: Migrate away from using getSemanticDiff.
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<Status> from,
      @Nonnull Aspect<Status> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      Status baseStatus,
      Status targetStatus,
      String entityUrn,
      AuditStamp auditStamp) {

    // If the new status is "removed", then return a soft-deletion event.
    if (isRemoved(targetStatus)) {
      return Collections.singletonList(
          ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.SOFT_DELETE)
            .auditStamp(auditStamp)
            .entityUrn(entityUrn).build());
    }

    // If the new status is "unremoved", then return an reinstatement event.
    if (!isRemoved(targetStatus)) {
      return Collections.singletonList(
          ChangeEvent.builder()
              .category(ChangeCategory.LIFECYCLE)
              .operation(ChangeOperation.REINSTATE)
              .auditStamp(auditStamp)
              .entityUrn(entityUrn).build());
    }

    return Collections.emptyList();
  }

  private boolean isRemoved(@Nullable final Status status) {
    return status != null && status.isRemoved();
  }
}
