\set dir `pwd`
\set file '''':dir'/input/few-edges.txt'''

create table edges (source int not null, dest int not null) segmented by hash(source,dest) all nodes;

\timing
copy edges from :file direct delimiter ' ';

select count(*)
  from edges e1
  join edges e2 on e1.dest = e2.source and e1.source < e2.source
  join edges e3 on e2.dest = e3.source and e3.dest = e1.source and e2.source < e3.source;
\timing
