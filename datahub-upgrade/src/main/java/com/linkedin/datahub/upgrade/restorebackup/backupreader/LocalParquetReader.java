package com.linkedin.datahub.upgrade.restorebackup.backupreader;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeContext;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;


/**
 * BackupReader for retrieving EbeanAspectV2 objects from a local parquet file
 */
@Slf4j
public class LocalParquetReader implements BackupReader {

  public LocalParquetReader() {
    // Need below to solve issue with hadoop path class not working in linux systems
    // https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hduser"));
  }

  @Override
  public String getName() {
    return "LOCAL_PARQUET";
  }

  @Nonnull
  @Override
  public EbeanAspectBackupIterator getBackupIterator(UpgradeContext context) {
    Optional<String> path = context.parsedArgs().get("BACKUP_FILE_PATH");
    if (!path.isPresent()) {
      context.report().addLine("BACKUP_FILE_PATH must be set to run RestoreBackup through local parquet file");
      throw new IllegalArgumentException(
          "BACKUP_FILE_PATH must be set to run RestoreBackup through local parquet file");
    }

    try {
      ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(path.get())).build();
      return new ParquetEbeanAspectBackupIterator(ImmutableList.of(reader));
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to build ParquetReader: %s", e));
    }
  }
}
