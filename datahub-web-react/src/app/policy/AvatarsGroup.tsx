import React from 'react';
import { CorpGroup, CorpUser, EntityType, Maybe } from '../../types.generated';
import EntityRegistry from '../entity/EntityRegistry';
import { CustomAvatar } from '../shared/avatar';
import { SpacedAvatarGroup } from '../shared/avatar/SpaceAvatarGroup';

type Props = {
    users?: Maybe<Array<CorpUser>>;
    groups?: Maybe<Array<CorpGroup>>;
    entityRegistry: EntityRegistry;
    maxCount?: number;
    size?: number;
};

/**
 * Component used for displaying the users and groups in policies table
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export default function AvatarsGroup({ users, groups, entityRegistry, maxCount = 6, size }: Props) {
    return (
        <SpacedAvatarGroup maxCount={maxCount}>
            {users &&
                users?.length > 0 &&
                (users || [])?.map((user, key) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <div data-testid={`avatar-tag-${user.urn}`} key={`${user.urn}-${key}`}>
                        <CustomAvatar
                            size={size}
                            name={entityRegistry.getDisplayName(EntityType.CorpUser, user)}
                            url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user.urn}`}
                            photoUrl={
                                user?.editableProperties?.pictureLink || user?.editableInfo?.pictureLink || undefined
                            }
                        />
                    </div>
                ))}
            {groups &&
                groups.length > 0 &&
                (groups || [])?.map((group, key) => (
                    // eslint-disable-next-line react/no-array-index-key
                    <div data-testid={`avatar-tag-${group.urn}`} key={`${group.urn}-${key}`}>
                        <CustomAvatar
                            size={size}
                            name={entityRegistry.getDisplayName(EntityType.CorpGroup, group)}
                            url={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${group.urn}`}
                            isGroup
                        />
                    </div>
                ))}
        </SpacedAvatarGroup>
    );
}
