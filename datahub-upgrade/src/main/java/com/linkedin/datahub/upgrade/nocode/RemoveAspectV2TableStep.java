package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import io.ebean.EbeanServer;
import java.util.function.Function;


/**
 * Optional step for removing Aspect V2 table.
 */
public class RemoveAspectV2TableStep implements UpgradeStep {

  private final EbeanServer _server;

  public RemoveAspectV2TableStep(final EbeanServer server) {
    _server = server;
  }

  @Override
  public String id() {
    return "RemoveAspectV2TableStep";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      context.report().addLine("Cleanup requested. Dropping metadata_aspect_v2");
      _server.execute(_server.createSqlUpdate("DROP TABLE IF EXISTS metadata_aspect_v2"));
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (context.parsedArgs().containsKey(NoCodeUpgrade.CLEAN_ARG_NAME)) {
      return false;
    }
    context.report().addLine("Cleanup has not been requested.");
    return true;
  }
}
