package com.ringcentral.analytics.etl.fs

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

class FileSystemService(fileSystem: FileSystem) {

    private val log = LoggerFactory.getLogger(classOf[FileSystemService])

    def moveFolder(tableName: String, originalPath: String, tsPath: String): Boolean = {

        val src = new Path(originalPath)
        val tmp = new Path(s"${originalPath}_tmp")
        val dst = new Path(s"$originalPath/$tsPath")
        log.info(s"Start moving data for $tableName from $src to $dst")
        val renameToTmpStatus = fileSystem.rename(src, tmp)
        if (!renameToTmpStatus) {
            log.error(s"Copy data for $tableName from ${src.toString} to ${tmp.toString} was not successful")
            return false
        }

        fileSystem.mkdirs(new Path(s"$originalPath"))
        val finalRenameStatus = fileSystem.rename(tmp, dst)
        if (!finalRenameStatus) {
            log.error(s"Copy data for $tableName from ${tmp.toString} to ${dst.toString} was not successful")
            return false
        }
        true
    }

    def moveFolderBack(tableName: String, originalPath: String, tsPath: String): Boolean = {
        val src = new Path(s"$originalPath/$tsPath")
        val tmp = new Path(s"${originalPath}_tmp")
        val dst = new Path(originalPath)
        log.info(s"Start moving data for $tableName from $src to $dst")
        val renameToTmpStatus = fileSystem.rename(src, tmp)
        if (!renameToTmpStatus) {
            log.error(s"Copy data for $tableName from ${src.toString} to ${tmp.toString} was not successful")
            return false
        }
        fileSystem.delete(dst, true)
        val finalRenameStatus = fileSystem.rename(tmp, dst)
        if (!finalRenameStatus) {
            log.error(s"Copy data for $tableName from ${tmp.toString} to ${dst.toString} was not successful")
            return false
        }
        true
    }


    def clearExcluding(folder: Path, exclusion: Path): Boolean = {
        val subfolders = fileSystem.listStatus(folder).map(_.getPath)

        if (!subfolders.contains(exclusion)) {
            log.error(s"Folder ${folder.toString} doesn't contain path ${exclusion.toString}. Something goes wrong")
            return false
        }

        subfolders.filter(path => path != exclusion)
            .filter(isNotTsSubfolder)
            .map(deleteFolder)
            .forall(identity)
    }

    private def isNotTsSubfolder(location: Path) = {
        !location.toString.split("/").last.startsWith("ts=")
    }

    private def deleteFolder(path: Path): Boolean = {
        if (fileSystem.delete(path, true)) {
            log.info(s"Folder $path was successfully deleted")
            true
        } else {
            log.error(s"Deletion for folder $path was failed")
            false
        }
    }
}
