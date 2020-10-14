package com.ringcentral.analytics.etl.fs

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging

class FileSystemService(fileSystem: FileSystem) extends Logging {
    def moveFolder(tableName: String, originalPath: String, tsPath: String): Boolean = {
        val src = new Path(originalPath)
        val tmp = new Path(s"${originalPath}_tmp")
        val dst = new Path(s"$originalPath/$tsPath")
        logInfo(s"Start moving data for $tableName from $src to $dst")
        val renameToTmpStatus = fileSystem.rename(src, tmp)
        if (!renameToTmpStatus) {
            logError(s"Copy data for $tableName from ${src.toString} to ${tmp.toString} was not successful")
            return false
        }

        fileSystem.mkdirs(new Path(s"$originalPath"))
        val finalRenameStatus = fileSystem.rename(tmp, dst)
        if (!finalRenameStatus) {
            logError(s"Copy data for $tableName from ${tmp.toString} to ${dst.toString} was not successful")
            return false
        }
        true
    }
}
