/*
 * Filehunter: a java utility library aimed to seamlessly and safely process files from any location whether local or remote
 * Copyright (C) 2018  Francesco Ciacca francesco@skillbill.it
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */

package net.skillbill.filehunter;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.skillbill.filehunter.dao.FileProcessedRepository;
import net.skillbill.filehunter.model.FileProcessed;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.*;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;

@Getter
@Setter
@Slf4j
@RequiredArgsConstructor
public class FileHunter {

    private final FileProcessedRepository fileProcessedRepository;

    private final  FileProcessor fileProcessor;
    private final  String fileContext;
    private final  String vfsRoot;
    private final  String basePath;
    private final  String filePattern;
    private final  String smbDomain;
    private final  String smbUser;
    private final  String smbPassword;

    private final HashMap<String,FileProcessed> fmap = new HashMap<>();
    private final HashMap<String,FileProcessed> md5map = new HashMap<>();

    private int newFilesFound = 0;
    private int noNewFilesFound = 0;
    private int filesLoaded = 0;
    private int updateFileReferences = 0;

    public void execute() {


        List<FileProcessed> all = fileProcessedRepository.findAll(fileContext);
        fmap.clear();
        md5map.clear();
        for (FileProcessed fileContributi : all) {
            fmap.put(fileContributi.getURL(),fileContributi);
            md5map.put(fileContributi.getMd5sum(),fileContributi);

        }

        try {
            //download dei file via VFS
            FileSystemOptions opts = new FileSystemOptions();
            FileSystemManager fsManager = VFS.getManager();

            //SMB workaround
            if(vfsRoot.toLowerCase().startsWith("smb://")) {
                StaticUserAuthenticator auth = new StaticUserAuthenticator(smbDomain, smbUser, smbPassword);
                DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(opts, auth);
                if (!fsManager.hasProvider("smb")) throw new RuntimeException("Provide missing SMB support");
            }


            FileObject fileObject = fsManager.resolveFile(vfsRoot,opts);
            fileObject = fileObject.resolveFile(basePath);

            processDir(filePattern, fileObject);

        } catch (Exception ex) {
            log.error("Error processing file",ex);
            throw  new RuntimeException(ex);
        }
    }

    private void processDir(String filePattern, FileObject fileObject) throws IOException, DigestException, NoSuchAlgorithmException {
        if(!fileObject.isFolder()){
            if(fileObject.isFile()){
                throw new IllegalArgumentException("Error: provided file is not a directory: "+fileObject.getURL());
            }
            else{
                throw new IllegalArgumentException("Error: unable to retrieve file object: "+fileObject.getURL());
            }
        }
        System.out.println("dir is "+fileObject.getURL());
        FileObject[] children = fileObject.getChildren();
        for (FileObject child : children) {
            if(child.isFile()) {
                if(child.getName().getBaseName().matches(filePattern)){
                    processFile(child);
                }
            }
            else{
                processDir(filePattern, child);
            }

        }
    }

    // see this How-to for a faster way to convert
    // a byte array to a HEX string
    public static String getMD5HexChecksum(byte[] b) {
        String result = "";

        for (int i=0; i < b.length; i++) {
            result += Integer.toString( ( b[i] & 0xff ) + 0x100, 16).substring( 1 );
        }
        return result;
    }

    private void processFile(FileObject child) throws IOException, NoSuchAlgorithmException, DigestException {
        String fileUrl = child.getURL().toExternalForm();
        log.info("processing file :"+ fileUrl);
        FileProcessed fileProcessed = fmap.get(fileUrl);
        FileContent fileContent = child.getContent();
        if (fileProcessed == null || fileProcessed.getLastModifyTime() == null|| fileContent.getLastModifiedTime() > localDateTimeToMills(  fileProcessed.getLastModifyTime()) || fileContent.getSize() != fileProcessed.getSize()){
            log.info("file:"+fileUrl+" is new or modified, loading it");
            InputStream content = fileContent.getInputStream();
            File tempFile = File.createTempFile("contributo_", ".EDF");
            byte[] buf = new byte[1024];
            FileOutputStream fout = new FileOutputStream(tempFile);
            int readen= -1;
            MessageDigest md = MessageDigest.getInstance("MD5");
            while((readen = content.read(buf)) != -1){
                fout.write(buf,0,readen);
                if(readen != -1){
                    md.update(buf,0,readen);
                }
            }
            content.close();
            fout.flush();
            fout.close();

            String md5HexChecksum = getMD5HexChecksum(md.digest());
            if(md5map.containsKey(md5HexChecksum)){
                FileProcessed sameMd5FileProcessed = md5map.get(md5HexChecksum);
                log.info("file:"+fileUrl+" was already loaded from another URL:"+sameMd5FileProcessed.getURL());
                fileProcessed = sameMd5FileProcessed;
            }

            LocalDateTime loadTime;
            String resulLog;
            String resulErrorLog;
            if(fileProcessed == null || !md5HexChecksum.equals( fileProcessed.getMd5sum())) {
                loadTime = LocalDateTime.now();
                log.info(" going to load contributi from file:"+fileUrl);
                FileProcessorResult result = fileProcessor.processFile(tempFile, fileUrl);
                resulLog = result.getMessage();
                resulErrorLog = result.getErrorMessage();
                filesLoaded++;
            }
            else {
                loadTime = fileProcessed.getLoadTime();
                resulLog = fileProcessed.getMessage();
                resulErrorLog = fileProcessed.getErrorMessage();

            }
            if(fileProcessed == null){
                fileProcessed = new FileProcessed(fileUrl, millsToLocalDateTime( fileContent.getLastModifiedTime()), loadTime, fileContent.getSize(), md5HexChecksum, resulLog, resulErrorLog);
                log.info(" going to load file:"+fileProcessed);
                fileProcessedRepository.create(fileProcessed, fileContext);
                newFilesFound++;
            }
            else{
                fileProcessed.setLastModifyTime(millsToLocalDateTime( fileContent.getLastModifiedTime()));
                fileProcessed.setSize(fileContent.getSize());
                fileProcessed.setMd5sum(md5HexChecksum);
                fileProcessed.setLoadTime(loadTime);
                fileProcessed.setURL(fileUrl);
                fileProcessed.setMessage (resulLog);
                fileProcessed.setErrorMessage(resulErrorLog);

                log.info(" going to update file:"+fileProcessed);
                fileProcessedRepository.update(fileProcessed);
                noNewFilesFound++;
                updateFileReferences++;
            }
            tempFile.delete();
        } else {
            noNewFilesFound++;
        }
    }
    public static LocalDateTime millsToLocalDateTime(long millis) {
        Instant instant = Instant.ofEpochMilli(millis);
        LocalDateTime date = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        return date;
    }
    private static long localDateTimeToMills(LocalDateTime date) {
        return date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
