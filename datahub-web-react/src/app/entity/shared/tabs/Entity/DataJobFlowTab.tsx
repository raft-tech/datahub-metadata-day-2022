import React from 'react';
import { useBaseEntity } from '../../EntityContext';
import { EntityType } from '../../../../../types.generated';
import { EntityList } from './components/EntityList';
import { useEntityRegistry } from '../../../../useEntityRegistry';

export const DataJobFlowTab = () => {
    const entity = useBaseEntity() as any;
    const dataJob = entity && entity.dataJob;
    const dataFlows = dataJob?.parentFlow?.relationships.map((relationship) => relationship.entity);
    const entityRegistry = useEntityRegistry();
    const title = `Part of ${entityRegistry.getEntityName(EntityType.DataFlow)}`;
    return <EntityList title={title} type={EntityType.DataFlow} entities={dataFlows || []} />;
};
