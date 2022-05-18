import { Typography, Button, message } from 'antd';
import React from 'react';
import { PlusOutlined } from '@ant-design/icons';
import { useEntityData, useRefetch } from '../../../EntityContext';
import { SidebarHeader } from './SidebarHeader';
import { useVisualizeWithSupersetMutation } from '../../../../../../graphql/mutations.generated';

export const SidebarVisualizeWithSupersetSection = () => {
    const [visualizeWithSuperset] = useVisualizeWithSupersetMutation();

    const { urn, entityData } = useEntityData();
    const refetch = useRefetch();

    return (
        <div>
            <SidebarHeader title="Visualize" />
            <div>
                <>
                    <Typography.Paragraph type="secondary">
                        Retrieve a Data Product for this Dataset.
                    </Typography.Paragraph>
                    <Button
                        type="default"
                        onClick={() =>
                            visualizeWithSuperset({
                                variables: {
                                    input: {
                                        name: entityData!.name!,
                                        qualifiedName: urn,
                                    },
                                },
                            })
                                .then((result) => {
                                    const url = result.data!.visualizeWithSuperset!;
                                    console.log(url);
                                    if (url !== '') {
                                        // TODO
                                        message.success({ content: `Visualizing datset: ${entityData!.name!}.` });
                                        window.open(url);
                                    } else {
                                        message.error({ content: `Failed to visualize datset.` });
                                    }
                                })
                                .then(refetch)
                                .catch((e) => {
                                    message.destroy();
                                    message.error({
                                        content: `Exception: \n ${e.message || ''}`,
                                        duration: 3,
                                    });
                                })
                        }
                    >
                        <PlusOutlined /> Visualize with Superset
                    </Button>
                </>
            </div>
        </div>
    );
};
