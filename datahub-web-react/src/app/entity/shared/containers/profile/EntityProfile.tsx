import React, { useCallback, useState } from 'react';
import { Alert, Divider } from 'antd';
import { MutationHookOptions, MutationTuple, QueryHookOptions, QueryResult } from '@apollo/client/react/types/types';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { EntityType, Exact } from '../../../../../types.generated';
import { Message } from '../../../../shared/Message';
import { getDataForEntityType, getEntityPath, useRoutedTab } from './utils';
import { EntitySidebarSection, EntityTab, GenericEntityProperties, GenericEntityUpdate } from '../../types';
import { EntityProfileNavBar } from './nav/EntityProfileNavBar';
import { ANTD_GRAY } from '../../constants';
import { EntityHeader } from './header/EntityHeader';
import { EntityTabs } from './header/EntityTabs';
import { EntitySidebar } from './sidebar/EntitySidebar';
import EntityContext from '../../EntityContext';
import useIsLineageMode from '../../../../lineage/utils/useIsLineageMode';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import LineageExplorer from '../../../../lineage/LineageExplorer';
import CompactContext from '../../../../shared/CompactContext';
import DynamicTab from '../../tabs/Entity/weaklyTypedAspects/DynamicTab';
import analytics, { EventType } from '../../../../analytics';
import { ProfileSidebarResizer } from './sidebar/ProfileSidebarResizer';

type Props<T, U> = {
    urn: string;
    entityType: EntityType;
    useEntityQuery: (
        baseOptions: QueryHookOptions<
            T,
            Exact<{
                urn: string;
            }>
        >,
    ) => QueryResult<
        T,
        Exact<{
            urn: string;
        }>
    >;
    useUpdateQuery?: (
        baseOptions?: MutationHookOptions<U, { urn: string; input: GenericEntityUpdate }> | undefined,
    ) => MutationTuple<U, { urn: string; input: GenericEntityUpdate }>;
    getOverrideProperties: (T) => GenericEntityProperties;
    tabs: EntityTab[];
    sidebarSections: EntitySidebarSection[];
    showDeprecateOption?: boolean;
};

const ContentContainer = styled.div`
    display: flex;
    height: auto;
    min-height: 100%;
    flex: 1;
`;

const HeaderAndTabs = styled.div`
    flex-grow: 1;
    min-width: 640px;
    height: 100%;
`;

const HeaderAndTabsFlex = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    height: 100%;
    max-height: 100%;
    overflow: hidden;
    min-height: 0;
    overflow-y: auto;

    &::-webkit-scrollbar {
        height: 12px;
        width: 2px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
`;
const Sidebar = styled.div<{ $width: number }>`
    max-height: 100%;
    overflow: auto;
    flex-basis: ${(props) => props.$width}px;
    padding-left: 20px;
    padding-right: 20px;
`;

const Header = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 20px 20px 0 20px;
    flex-shrink: 0;
`;

const TabContent = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
    overflow: auto;
`;

const defaultTabDisplayConfig = {
    visible: (_, _1) => true,
    enabled: (_, _1) => true,
};

const defaultSidebarSection = {
    visible: (_, _1) => true,
};

const INITIAL_SIDEBAR_WIDTH = 400;
const MAX_SIDEBAR_WIDTH = 800;
const MIN_SIDEBAR_WIDTH = 200;

/**
 * Container for display of the Entity Page
 */
export const EntityProfile = <T, U>({
    urn,
    useEntityQuery,
    useUpdateQuery,
    entityType,
    getOverrideProperties,
    tabs,
    sidebarSections,
    showDeprecateOption,
}: Props<T, U>): JSX.Element => {
    const isLineageMode = useIsLineageMode();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const isCompact = React.useContext(CompactContext);
    const tabsWithDefaults = tabs.map((tab) => ({ ...tab, display: { ...defaultTabDisplayConfig, ...tab.display } }));
    const sideBarSectionsWithDefaults = sidebarSections.map((sidebarSection) => ({
        ...sidebarSection,
        display: { ...defaultSidebarSection, ...sidebarSection.display },
    }));

    const [sidebarWidth, setSidebarWidth] = useState(INITIAL_SIDEBAR_WIDTH);

    const routeToTab = useCallback(
        ({
            tabName,
            tabParams,
            method = 'push',
        }: {
            tabName: string;
            tabParams?: Record<string, any>;
            method?: 'push' | 'replace';
        }) => {
            analytics.event({
                type: EventType.EntitySectionViewEvent,
                entityType,
                entityUrn: urn,
                section: tabName.toLowerCase(),
            });
            history[method](getEntityPath(entityType, urn, entityRegistry, false, tabName, tabParams));
        },
        [history, entityType, urn, entityRegistry],
    );

    const { loading, error, data, refetch } = useEntityQuery({
        variables: { urn },
    });

    const maybeUpdateEntity = useUpdateQuery?.({
        onCompleted: () => refetch(),
    });
    let updateEntity;
    if (maybeUpdateEntity) {
        [updateEntity] = maybeUpdateEntity;
    }

    const entityData =
        (data && getDataForEntityType({ data: data[Object.keys(data)[0]], entityType, getOverrideProperties })) || null;

    const lineage = entityData ? entityRegistry.getLineageVizConfig(entityType, entityData) : undefined;

    const autoRenderTabs: EntityTab[] =
        entityData?.autoRenderAspects?.map((aspect) => ({
            name: aspect.renderSpec?.displayName || aspect.aspectName,
            component: () => (
                <DynamicTab
                    renderSpec={aspect.renderSpec}
                    type={aspect.renderSpec?.displayType}
                    payload={aspect.payload}
                />
            ),
            display: {
                visible: () => true,
                enabled: () => true,
            },
        })) || [];

    const routedTab = useRoutedTab([...tabsWithDefaults, ...autoRenderTabs]);

    if (isCompact) {
        return (
            <EntityContext.Provider
                value={{
                    urn,
                    entityType,
                    entityData,
                    baseEntity: data,
                    updateEntity,
                    routeToTab,
                    refetch,
                    lineage,
                }}
            >
                <div>
                    {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
                    {!loading && error && (
                        <Alert type="error" message={error?.message || `Entity failed to load for urn ${urn}`} />
                    )}
                    {!loading && (
                        <>
                            <EntityHeader showDeprecateOption={showDeprecateOption} />
                            <Divider />
                            <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} />
                        </>
                    )}
                </div>
            </EntityContext.Provider>
        );
    }

    const isBrowsable = entityRegistry.getBrowseEntityTypes().includes(entityType);
    const isLineageEnabled = entityRegistry.getLineageEntityTypes().includes(entityType);
    const showBrowseBar = isBrowsable || isLineageEnabled;

    return (
        <EntityContext.Provider
            value={{
                urn,
                entityType,
                entityData,
                baseEntity: data,
                updateEntity,
                routeToTab,
                refetch,
                lineage,
            }}
        >
            <>
                {showBrowseBar && <EntityProfileNavBar urn={urn} entityType={entityType} />}
                {entityData?.status?.removed === true && (
                    <Alert
                        message="This entity is not discoverable via search or lineage graph. Contact your DataHub admin for more information."
                        banner
                    />
                )}
                {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
                {!loading && error && (
                    <Alert type="error" message={error?.message || `Entity failed to load for urn ${urn}`} />
                )}
                <ContentContainer>
                    {isLineageMode ? (
                        <LineageExplorer type={entityType} urn={urn} />
                    ) : (
                        <>
                            <HeaderAndTabs>
                                <HeaderAndTabsFlex>
                                    <Header>
                                        <EntityHeader showDeprecateOption={showDeprecateOption} />
                                        <EntityTabs
                                            tabs={[...tabsWithDefaults, ...autoRenderTabs]}
                                            selectedTab={routedTab}
                                        />
                                    </Header>
                                    <TabContent>
                                        {routedTab && <routedTab.component properties={routedTab.properties} />}
                                    </TabContent>
                                </HeaderAndTabsFlex>
                            </HeaderAndTabs>
                            <ProfileSidebarResizer
                                setSidePanelWidth={(width) =>
                                    setSidebarWidth(Math.min(Math.max(width, MIN_SIDEBAR_WIDTH), MAX_SIDEBAR_WIDTH))
                                }
                                initialSize={sidebarWidth}
                            />
                            <Sidebar $width={sidebarWidth}>
                                <EntitySidebar sidebarSections={sideBarSectionsWithDefaults} />
                            </Sidebar>
                        </>
                    )}
                </ContentContainer>
            </>
        </EntityContext.Provider>
    );
};
