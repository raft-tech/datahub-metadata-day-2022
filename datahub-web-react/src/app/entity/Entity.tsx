import { EntityType, SearchResult } from '../../types.generated';
import { FetchedEntity } from '../lineage/types';
import { GenericEntityProperties } from './shared/types';

export enum PreviewType {
    /**
     * A preview shown within the search experience
     */
    SEARCH,
    /**
     * A preview shown within the browse experience
     */
    BROWSE,
    /**
     * A generic preview shown within other entity pages, etc.
     */
    PREVIEW,
    /**
     * A tiny search preview for text-box search.
     */
    MINI_SEARCH,
}

export enum IconStyleType {
    /**
     * Colored Icon
     */
    HIGHLIGHT,
    /**
     * Grayed out icon
     */
    ACCENT,
    /**
     * Rendered in a Tab pane header
     */
    TAB_VIEW,
    /**
     * Rendered in Lineage as default
     */
    SVG,
}

/**
 * Base interface used for authoring DataHub Entities on the client side.
 *
 * <T> the generated GraphQL data type associated with the entity.
 */
export interface Entity<T> {
    /**
     * Corresponding GQL EntityType.
     */
    type: EntityType;

    /**
     * Ant-design icon associated with the Entity. For a list of all candidate icons, see
     * https://ant.design/components/icon/
     */
    icon: (fontSize: number, styleType: IconStyleType) => JSX.Element;

    /**
     * Returns whether the entity search is enabled
     */
    isSearchEnabled: () => boolean;

    /**
     * Returns whether the entity browse is enabled
     */
    isBrowseEnabled: () => boolean;

    /**
     * Returns whether the entity browse is enabled
     */
    isLineageEnabled: () => boolean;

    /**
     * Returns the name of the entity as it appears in a URL, e.g. '/dataset/:urn'.
     */
    getPathName: () => string;

    /**
     * Returns the plural name of the entity used when displaying collections (search, browse results), e.g. 'Datasets'.
     */
    getCollectionName: () => string;

    /**
     * Returns the singular name of the entity used when referring to an individual
     */
    getEntityName?: () => string;

    /**
     * Renders the 'profile' of the entity on an entity details page.
     *
     * TODO: Explore using getGenericEntityProperties for rendering profiles.
     */
    renderProfile: (urn: string) => JSX.Element;

    /**
     * Renders a preview of the entity across different use cases like search, browse, etc.
     *
     * TODO: Explore using getGenericEntityProperties for rendering previews.
     */
    renderPreview: (type: PreviewType, data: T) => JSX.Element;

    /**
     * Renders a search result
     *
     * TODO: Explore using getGenericEntityProperties for rendering profiles.
     */
    renderSearch: (result: SearchResult) => JSX.Element;

    /**
     * Constructs config to add entity to lineage viz
     */
    getLineageVizConfig?: (entity: T) => FetchedEntity;

    /**
     * Returns a display name for the entity
     *
     * TODO: Migrate to using getGenericEntityProperties for display name retrieval.
     */
    displayName: (data: T) => string;

    /**
     * Returns generic entity properties for the entity
     */
    getGenericEntityProperties: (data: T) => GenericEntityProperties | null;
}
