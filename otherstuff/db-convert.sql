-- this is here so I can convert from databases that used the old cb-scala-unduplicator
-- format.  you shouldn't need it for normal use of fuse-sha1
begin transaction;
create temporary table files_backup(
  path varchar not null primary key,
  chksum varchar not null);
insert into files_backup select path, chksum from files;
drop table files;
create table files(
  path varchar not null primary key,
  chksum varchar not null);
insert into files select path, chksum from files_backup;
drop table files_backup;
commit;
vacuum;
