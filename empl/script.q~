use hiveprac;
drop table if exists student_marks;
create table student_marks(name string,subjectId  int,marks int) 
  row format delimited
  fields terminated by ','
  stored as textfile;
load data inpath '${INPUT}' overwrite into table student_marks;
insert overwrite directory '${OUTPUT}' select name,' ',subjectId,' ',marks from student_marks;
