# filehunter
a java utility library aimed to seamlessly and safely process files from any location whether local or remote
It allows to pickup files to any URL-able location, like file://.., ftp://.. and so on.
Currently it uses all the protocols supported by the apache-vfs project, including smb protocl for accessing Windows share.
It let you safely process only new or updated files, detecting those conditions not onlylooking at file metadatas but also comparing hashes of the files.
It keeps track of already visted files using a local sqlite database
