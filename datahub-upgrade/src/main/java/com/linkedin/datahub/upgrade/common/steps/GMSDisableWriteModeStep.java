package com.linkedin.datahub.upgrade.common.steps;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.entity.client.RestliEntityClient;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class GMSDisableWriteModeStep implements UpgradeStep {

  private final Authentication _systemAuthentication;
  private final RestliEntityClient _entityClient;

  @Override
  public String id() {
    return "GMSDisableWriteModeStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        _entityClient.setWritable(false, _systemAuthentication);
      } catch (Exception e) {
        e.printStackTrace();
        context.report().addLine("Failed to turn write mode off in GMS");
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }
}
