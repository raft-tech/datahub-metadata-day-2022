# Restoring Search and Graph Indices from Local Database

If search or graph services go down or you have made changes to them that require reindexing, you can restore them from
the aspects stored in the local database.

When a new version of the aspect gets ingested, GMS initiates an MAE event for the aspect which is consumed to update
the search and graph indices. As such, we can fetch the latest version of each aspect in the local database and produce
MAE events corresponding to the aspects to restore the search and graph indices.

## Docker-compose

Run the following command from root to send MAE for each aspect in the Local DB.

```
./docker/datahub-upgrade/datahub-upgrade.sh -u RestoreIndices
```

If you need to clear the search and graph indices before restoring, add `-a clean` to the end of the command.

Refer to this [doc](../../docker/datahub-upgrade/README.md#environment-variables) on how to set environment variables
for your environment.

## Kubernetes

Run `kubectl get cronjobs` to see if the restoration job template has been deployed. If you see results like below, you
are good to go.

```
NAME                                          SCHEDULE    SUSPEND   ACTIVE   LAST SCHEDULE   AGE
datahub-datahub-cleanup-job-template          * * * * *   True      0        <none>          2d3h
datahub-datahub-restore-indices-job-template  * * * * *   True      0        <none>          2d3h
```

If not, deploy latest helm charts to use this functionality.

Once restore indices job template has been deployed, run the following command to start a job that restores indices.

```
kubectl create job --from=cronjob/datahub-datahub-restore-indices-job-template datahub-restore-indices-adhoc
```

Once the job completes, your indices will have been restored. 