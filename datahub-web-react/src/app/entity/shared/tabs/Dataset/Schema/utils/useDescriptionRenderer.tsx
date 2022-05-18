import React from 'react';
import { EditableSchemaMetadata, SchemaField, SubResourceType } from '../../../../../../../types.generated';
import DescriptionField from '../../../../../dataset/profile/schema/components/SchemaDescriptionField';
import { pathMatchesNewPath } from '../../../../../dataset/profile/schema/utils/utils';
import { useUpdateDescriptionMutation } from '../../../../../../../graphql/mutations.generated';
import { useEntityData, useRefetch } from '../../../../EntityContext';

export default function useDescriptionRenderer(editableSchemaMetadata: EditableSchemaMetadata | null | undefined) {
    const { urn } = useEntityData();
    const refetch = useRefetch();
    const [updateDescription] = useUpdateDescriptionMutation();

    return (description: string, record: SchemaField): JSX.Element => {
        const relevantEditableFieldInfo = editableSchemaMetadata?.editableSchemaFieldInfo.find(
            (candidateEditableFieldInfo) => pathMatchesNewPath(candidateEditableFieldInfo.fieldPath, record.fieldPath),
        );

        return (
            <DescriptionField
                description={relevantEditableFieldInfo?.description || description}
                original={record.description}
                isEdited={!!relevantEditableFieldInfo?.description}
                onUpdate={(updatedDescription) =>
                    updateDescription({
                        variables: {
                            input: {
                                description: updatedDescription,
                                resourceUrn: urn,
                                subResource: record.fieldPath,
                                subResourceType: SubResourceType.DatasetField,
                            },
                        },
                    }).then(refetch)
                }
            />
        );
    };
}
