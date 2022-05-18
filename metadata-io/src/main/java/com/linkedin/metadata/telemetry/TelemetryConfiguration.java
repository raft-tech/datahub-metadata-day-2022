package com.linkedin.metadata.telemetry;

import lombok.Data;
/**
 * POJO representing the "telemtry" configuration block in application.yml.
 */
@Data
public class TelemetryConfiguration {
    /**
    * Whether cli telemtry is enabled
    */
    public boolean enabledCli;
    /**
    * Whether reporting telemetry is enabled
    */
    public boolean enabledIngestion;
    /**
     * Whether or not third party logging should be enabled for this instance
     */
    public boolean enableThirdPartyLogging;
}