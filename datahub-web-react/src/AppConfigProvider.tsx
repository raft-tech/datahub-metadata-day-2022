import React, { useEffect } from 'react';
import './App.less';
import { THIRD_PARTY_LOGGING_KEY } from './app/analytics/analytics';
import { checkAuthStatus } from './app/auth/checkAuthStatus';
import { AppConfigContext, DEFAULT_APP_CONFIG } from './appConfigContext';
import { useAppConfigQuery } from './graphql/app.generated';

const AppConfigProvider = ({ children }: { children: React.ReactNode }) => {
    const { data: appConfigData, refetch } = useAppConfigQuery();

    const refreshAppConfig = () => {
        refetch();
    };

    useEffect(() => {
        if (appConfigData && appConfigData.appConfig) {
            if (appConfigData.appConfig.telemetryConfig.enableThirdPartyLogging) {
                localStorage.setItem(THIRD_PARTY_LOGGING_KEY, 'true');
                checkAuthStatus(); // identify in analyitcs once we receive config response
            } else {
                localStorage.setItem(THIRD_PARTY_LOGGING_KEY, 'false');
            }
        }
    }, [appConfigData]);

    return (
        <AppConfigContext.Provider
            value={{ config: appConfigData?.appConfig || DEFAULT_APP_CONFIG, refreshContext: refreshAppConfig }}
        >
            {children}
        </AppConfigContext.Provider>
    );
};

export default AppConfigProvider;
