# DataHub Quickstart Guide

## Deploying DataHub

To deploy a new instance of DataHub, perform the following steps.

1. Install [docker](https://docs.docker.com/install/), [jq](https://stedolan.github.io/jq/download/) and [docker-compose](https://docs.docker.com/compose/install/) (if
   using Linux). Make sure to allocate enough hardware resources for Docker engine. Tested & confirmed config: 2 CPUs,
   8GB RAM, 2GB Swap area, and 10GB disk space.

2. Launch the Docker Engine from command line or the desktop app.

3. Install the DataHub CLI

   a. Ensure you have Python 3.6+ installed & configured. (Check using `python3 --version`)

   b. Run the following commands in your terminal

   ```
   python3 -m pip install --upgrade pip wheel setuptools
   python3 -m pip uninstall datahub acryl-datahub || true  # sanity check - ok if it fails
   python3 -m pip install --upgrade acryl-datahub
   datahub version
   ```

:::note

   If you see "command not found", try running cli commands with the prefix 'python3 -m' instead like `python3 -m datahub version`
   Note that DataHub CLI does not support Python 2.x.

:::

4. To deploy DataHub, run the following CLI command from your terminal

   ```
   datahub docker quickstart
   ```

   Upon completion of this step, you should be able to navigate to the DataHub UI
   at [http://localhost:9002](http://localhost:9002) in your browser. You can sign in using `datahub` as both the
   username and password.

5. To ingest the sample metadata, run the following CLI command from your terminal
   ```
   datahub docker ingest-sample-data
   ```

That's it! To start pushing your company's metadata into DataHub, take a look at
the [Metadata Ingestion Framework](../metadata-ingestion/README.md).

## Resetting DataHub

To cleanse DataHub of all of it's state (e.g. before ingesting your own), you can use the CLI `nuke` command.

```
datahub docker nuke
```

## Updating DataHub locally

If you have been testing DataHub locally, a new version of DataHub got released and you want to try the new version then you can use below commands. 

```
datahub docker nuke --keep-data
datahub docker quickstart
```

This will keep the data that you have ingested so far in DataHub and start a new quickstart with the latest version of DataHub.

## Troubleshooting

### Command not found: datahub

If running the datahub cli produces "command not found" errors inside your terminal, your system may be defaulting to an
older version of Python. Try prefixing your `datahub` commands with `python3 -m`:

```
python3 -m datahub docker quickstart
```

Another possibility is that your system PATH does not include pip's `$HOME/.local/bin` directory.  On linux, you can add this to your `~/.bashrc`:

```
if [ -d "$HOME/.local/bin" ] ; then
    PATH="$HOME/.local/bin:$PATH"
fi
```

### Miscellaneous Docker issues

There can be misc issues with Docker, like conflicting containers and dangling volumes, that can often be resolved by
pruning your Docker state with the following command. Note that this command removes all unused containers, networks,
images (both dangling and unreferenced), and optionally, volumes.

```
docker system prune
```
