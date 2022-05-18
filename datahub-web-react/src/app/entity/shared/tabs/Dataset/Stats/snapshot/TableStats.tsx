import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Maybe, UserUsageCounts } from '../../../../../../../types.generated';
import UsageFacepile from '../../../../../dataset/profile/UsageFacepile';
import { InfoItem } from '../../../../components/styled/InfoItem';
import { ANTD_GRAY } from '../../../../constants';
import { countFormatter, countSeparator } from '../../../../../../../utils/formatter/index';

type Props = {
    rowCount?: number;
    columnCount?: number;
    queryCount?: number;
    users?: Array<Maybe<UserUsageCounts>>;
    lastUpdatedTime?: string;
    lastReportedTime?: string;
};

const StatSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 16px 20px;
`;

const StatContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    z-index: 1;
    justify-content: ${(props) => props.justifyContent};
    padding: 12px 2px;
`;

export default function TableStats({
    rowCount,
    columnCount,
    queryCount,
    users,
    lastUpdatedTime,
    lastReportedTime,
}: Props) {
    // If there are less than 4 items, simply stack the stat views.
    const justifyContent = !queryCount && !users ? 'default' : 'space-between';
    const lastReportedTimeString = lastReportedTime || 'unknown';
    return (
        <StatSection>
            <Typography.Title level={5}>Table Stats</Typography.Title>
            <StatContainer justifyContent={justifyContent}>
                {rowCount && (
                    <InfoItem title="Rows">
                        <Tooltip title={countSeparator(rowCount)} placement="right">
                            <Typography.Text strong style={{ fontSize: 24 }}>
                                {countFormatter(rowCount)}
                            </Typography.Text>
                        </Tooltip>
                    </InfoItem>
                )}
                {columnCount && (
                    <InfoItem title="Columns">
                        <Typography.Text strong style={{ fontSize: 24 }}>
                            {columnCount}
                        </Typography.Text>
                    </InfoItem>
                )}
                {queryCount && (
                    <InfoItem title="Monthly Queries">
                        <Typography.Text strong style={{ fontSize: 24 }}>
                            {queryCount}
                        </Typography.Text>
                    </InfoItem>
                )}
                {users && (
                    <InfoItem title="Top Users">
                        <div style={{ paddingTop: 8 }}>
                            <UsageFacepile users={users} />
                        </div>
                    </InfoItem>
                )}
                {lastUpdatedTime && (
                    <InfoItem title="Last Updated" width="220px">
                        <Tooltip title={`Last reported at ${lastReportedTimeString}`}>
                            <Typography.Text strong style={{ fontSize: 16 }}>
                                {lastUpdatedTime}
                            </Typography.Text>
                        </Tooltip>
                    </InfoItem>
                )}
            </StatContainer>
        </StatSection>
    );
}
