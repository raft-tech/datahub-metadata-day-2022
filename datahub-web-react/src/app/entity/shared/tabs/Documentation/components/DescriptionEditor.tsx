import React, { useState, useEffect } from 'react';
import { message, Button } from 'antd';
import { CheckOutlined } from '@ant-design/icons';

import analytics, { EventType, EntityActionType } from '../../../../../analytics';

import StyledMDEditor from '../../../components/styled/StyledMDEditor';
import TabToolbar from '../../../components/styled/TabToolbar';

import { GenericEntityUpdate } from '../../../types';
import { useEntityData, useEntityUpdate, useRefetch } from '../../../EntityContext';
import { useUpdateDescriptionMutation } from '../../../../../../graphql/mutations.generated';
import { DiscardDescriptionModal } from './DiscardDescriptionModal';
import { EDITED_DESCRIPTIONS_CACHE_NAME } from '../../../utils';

export const DescriptionEditor = ({ onComplete }: { onComplete?: () => void }) => {
    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const updateEntity = useEntityUpdate<GenericEntityUpdate>();
    const [updateDescriptionMutation] = useUpdateDescriptionMutation();

    const localStorageDictionary = localStorage.getItem(EDITED_DESCRIPTIONS_CACHE_NAME);
    const editedDescriptions = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};
    const description = editedDescriptions.hasOwnProperty(urn)
        ? editedDescriptions[urn]
        : entityData?.editableProperties?.description || entityData?.properties?.description || '';

    const [updatedDescription, setUpdatedDescription] = useState(description);
    const [isDescriptionUpdated, setIsDescriptionUpdated] = useState(editedDescriptions.hasOwnProperty(urn));
    const [cancelModalVisible, setCancelModalVisible] = useState(false);

    const updateDescriptionLegacy = () => {
        return updateEntity?.({
            variables: { urn, input: { editableProperties: { description: updatedDescription || '' } } },
        });
    };

    const updateDescription = () => {
        return updateDescriptionMutation({
            variables: {
                input: {
                    description: updatedDescription,
                    resourceUrn: urn,
                },
            },
        });
    };

    const handleSaveDescription = async () => {
        message.loading({ content: 'Saving...' });
        try {
            if (updateEntity) {
                // Use the legacy update description path.
                await updateDescriptionLegacy();
            } else {
                // Use the new update description path.
                await updateDescription();
            }
            message.destroy();
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType,
                entityUrn: urn,
            });
            message.success({ content: 'Description Updated', duration: 2 });
            // Updating the localStorage after save
            delete editedDescriptions[urn];
            if (Object.keys(editedDescriptions).length === 0) {
                localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
            } else {
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions));
            }
            if (onComplete) onComplete();
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to update description: \n ${e.message || ''}`, duration: 2 });
            }
        }
        refetch?.();
    };

    // Function to handle all changes in Editor
    const handleEditorChange = (editedDescription: string) => {
        setUpdatedDescription(editedDescription);
        if (editedDescription === description) {
            setIsDescriptionUpdated(false);
        } else {
            setIsDescriptionUpdated(true);
        }
    };

    // Updating the localStorage when the user has paused for 5 sec
    useEffect(() => {
        let delayDebounceFn: ReturnType<typeof setTimeout>;
        const editedDescriptionsLocal = (localStorageDictionary && JSON.parse(localStorageDictionary)) || {};

        if (isDescriptionUpdated) {
            delayDebounceFn = setTimeout(() => {
                editedDescriptionsLocal[urn] = updatedDescription;
                localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptionsLocal));
            }, 5000);
        }
        return () => clearTimeout(delayDebounceFn);
    }, [urn, isDescriptionUpdated, updatedDescription, localStorageDictionary]);

    // Handling the Discard Modal
    const showModal = () => {
        if (isDescriptionUpdated) {
            setCancelModalVisible(true);
        } else if (onComplete) onComplete();
    };

    function onCancel() {
        setCancelModalVisible(false);
    }

    const onDiscard = () => {
        delete editedDescriptions[urn];
        if (Object.keys(editedDescriptions).length === 0) {
            localStorage.removeItem(EDITED_DESCRIPTIONS_CACHE_NAME);
        } else {
            localStorage.setItem(EDITED_DESCRIPTIONS_CACHE_NAME, JSON.stringify(editedDescriptions));
        }
        if (onComplete) onComplete();
    };

    return entityData ? (
        <>
            <TabToolbar>
                <Button type="text" onClick={showModal}>
                    Back
                </Button>
                <Button onClick={handleSaveDescription} disabled={!isDescriptionUpdated}>
                    <CheckOutlined /> Save
                </Button>
            </TabToolbar>
            <StyledMDEditor
                value={description}
                onChange={(v) => handleEditorChange(v || '')}
                preview="live"
                visiableDragbar={false}
            />
            {cancelModalVisible && (
                <DiscardDescriptionModal
                    cancelModalVisible={cancelModalVisible}
                    onDiscard={onDiscard}
                    onCancel={onCancel}
                />
            )}
        </>
    ) : null;
};
