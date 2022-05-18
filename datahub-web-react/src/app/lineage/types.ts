import {
    Chart,
    Dashboard,
    DataJob,
    Dataset,
    EntityType,
    MlFeatureTable,
    MlPrimaryKey,
    MlFeature,
    MlModel,
    MlModelGroup,
    Maybe,
    Status,
} from '../../types.generated';

export type EntitySelectParams = {
    type: EntityType;
    urn: string;
};

export type LineageExpandParams = {
    type: EntityType;
    urn: string;
    direction: Direction;
};

export type FetchedEntity = {
    urn: string;
    name: string;
    // name to be shown on expansion if available
    expandedName?: string;
    type: EntityType;
    subtype?: string;
    icon?: string;
    // children?: Array<string>;
    upstreamChildren?: Array<EntityAndType>;
    downstreamChildren?: Array<EntityAndType>;
    fullyFetched?: boolean;
    platform?: string;
    status?: Maybe<Status>;
};

export type NodeData = {
    urn?: string;
    name: string;
    // name to be shown on expansion if available
    expandedName?: string;
    type?: EntityType;
    subtype?: string;
    children?: Array<NodeData>;
    unexploredChildren?: number;
    icon?: string;
    // Hidden children are unexplored but in the opposite direction of the flow of the graph.
    // Currently our visualization does not support expanding in two directions
    countercurrentChildrenUrns?: string[];
    platform?: string;
    status?: Maybe<Status>;
};

export type VizNode = {
    x: number;
    y: number;
    data: NodeData;
};

export type VizEdge = {
    source: VizNode;
    target: VizNode;
    curve: { x: number; y: number }[];
};

export type FetchedEntities = { [x: string]: FetchedEntity };

export enum Direction {
    Upstream = 'Upstream',
    Downstream = 'Downstream',
}

export type LineageExplorerParams = {
    type: string;
    urn: string;
};

export type TreeProps = {
    margin?: { top: number; right: number; bottom: number; left: number };
    entityAndType?: EntityAndType | null;
    fetchedEntities: { [x: string]: FetchedEntity };
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    hoveredEntity?: EntitySelectParams;
};

export type EntityAndType =
    | {
          type: EntityType.Dataset;
          entity: Dataset;
      }
    | {
          type: EntityType.Chart;
          entity: Chart;
      }
    | {
          type: EntityType.Dashboard;
          entity: Dashboard;
      }
    | {
          type: EntityType.DataJob;
          entity: DataJob;
      }
    | {
          type: EntityType.MlfeatureTable;
          entity: MlFeatureTable;
      }
    | {
          type: EntityType.Mlfeature;
          entity: MlFeature;
      }
    | {
          type: EntityType.Mlmodel;
          entity: MlModel;
      }
    | {
          type: EntityType.MlmodelGroup;
          entity: MlModelGroup;
      }
    | {
          type: EntityType.MlprimaryKey;
          entity: MlPrimaryKey;
      };
