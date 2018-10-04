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

package net.skillbill.filehunter.model;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@ToString
@AllArgsConstructor
public class FileProcessed {

    public FileProcessed(String URL, LocalDateTime lastModifyTime, LocalDateTime loadTime, Long size, String md5sum, String importLog, String importMsg) {
        this.URL = URL;
        this.lastModifyTime = lastModifyTime;
        this.loadTime = loadTime;
        this.size = size;
        this.md5sum = md5sum;
        this.message = importLog;
        this.errorMessage = importMsg;
    }


    private Long id;
    private String URL;
    private LocalDateTime lastModifyTime;
    private LocalDateTime loadTime;
    private Long size;
    private String md5sum;

    private String message;
    private String errorMessage;


}
