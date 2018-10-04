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

package net.skillbill.filehunter.dao.impl;

import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.skillbill.filehunter.FileHunter;
import net.skillbill.filehunter.dao.FileProcessedRepository;
import net.skillbill.filehunter.model.FileProcessed;

import java.sql.*;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class FileProcessedRepositoryJdbcImpl implements FileProcessedRepository {

    public FileProcessedRepositoryJdbcImpl(String driverClassname, String url, String login, String password) {
        try {
            @Cleanup
            Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
            @Cleanup
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.

            statement.executeUpdate("CREATE TABLE IF NOT EXISTS processed_file (id INTEGER, url TEXT, lastModifyTime INTEGER, loadTime INTEGER, size INTEGER, md5sum TEXT, message TEXT, error_message TEXT);");
        } catch (Exception e) {
            log.error("error in db connection initialization",e);
            throw new RuntimeException("error in db connection initialization", e);
        }

    }


    @Override
    @SneakyThrows
    public FileProcessed create(FileProcessed in) {
        @Cleanup
        Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
        @Cleanup
        PreparedStatement pstmt = connection.prepareStatement("insert into processed_file values(?,?,?,?,?,?,?,?)");
        int i = 0;
        long id = System.nanoTime();
        pstmt.setLong(++i, id);
        pstmt.setString(++i,in.getURL());
        pstmt.setLong(++i,in.getLastModifyTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        pstmt.setLong(++i,in.getLoadTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        pstmt.setLong(++i,in.getSize());
        pstmt.setString(++i,in.getMd5sum());
        pstmt.setString(++i,in.getMessage());
        pstmt.setString(++i,in.getErrorMessage());

        pstmt.executeUpdate();
        in.setId(id);
        return in;
    }

    @Override
    @SneakyThrows
    public FileProcessed update(FileProcessed in) {
        @Cleanup
        Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
        @Cleanup
        PreparedStatement pstmt = connection.prepareStatement("update processed_file set url=?, lastModifyTime=?, loadTime=?, size=?, md5sum=?, message=?, error_message=? where id =?");
        int i = 0;
        pstmt.setString(++i,in.getURL());
        pstmt.setLong(++i,in.getLastModifyTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        pstmt.setLong(++i,in.getLoadTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        pstmt.setLong(++i,in.getSize());
        pstmt.setString(++i,in.getMd5sum());
        pstmt.setString(++i,in.getMessage());
        pstmt.setString(++i,in.getErrorMessage());
        pstmt.setLong(++i,in.getId());

        pstmt.executeUpdate();
        return in;
    }

    @Override
    @SneakyThrows
    public List<FileProcessed> findAll() {
        @Cleanup
        Connection connection = DriverManager.getConnection("jdbc:sqlite:sample.db");
        @Cleanup
        PreparedStatement pstmt = connection.prepareStatement("select * from processed_file");
        @Cleanup
        ResultSet rset =  pstmt.executeQuery();

        List<FileProcessed> res = new ArrayList<>();
        while(rset.next()){
            FileProcessed row = new FileProcessed(rset.getLong("id"), rset.getString("url"), FileHunter.millsToLocalDateTime( rset.getLong("lastModifyTime")), FileHunter.millsToLocalDateTime( rset.getLong("loadTime")), rset.getLong("size"),rset.getString("md5sum"),rset.getString("message"),rset.getString("error_message")  );
            res.add(row);
        }
        return res;
    }
}
