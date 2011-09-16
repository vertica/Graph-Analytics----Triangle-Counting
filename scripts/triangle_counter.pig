set default_parallel 24;
EDGES         = load 'input/few-edges.txt' using PigStorage(' ') as (source:long, dest:long);
CANON_EDGES_1 = filter EDGES by source < dest;
CANON_EDGES_2 = filter EDGES by source < dest;
TRIAD_JOIN    = join CANON_EDGES_1 by dest, CANON_EDGES_2 by source;
OPEN_EDGES    = foreach TRIAD_JOIN generate CANON_EDGES_1::source, CANON_EDGES_2::dest;
TRIANGLE_JOIN = join CANON_EDGES_1 by (source,dest), OPEN_EDGES by (CANON_EDGES_1::source, CANON_EDGES_2::dest);
TRIANGLES     = foreach TRIANGLE_JOIN generate 1 as a:int;
CONST_GROUP   = group TRIANGLES ALL parallel 1;
FINAL_COUNT   = foreach CONST_GROUP generate COUNT(TRIANGLES);

dump FINAL_COUNT;
