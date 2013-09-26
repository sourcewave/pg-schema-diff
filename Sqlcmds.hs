{-# LANGUAGE QuasiQuotes #-}

module Sqlcmds where

import Str(str)


schemaList = [str|
SELECT n.nspname AS "Name"
 -- ,pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;
|]

-- -----------------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------

tblList = [str| 
SELECT n.nspname AS "Schema", c.relname AS "Name", d.description AS "Comment",
  relacl AS "ACLs"
FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid 
  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)
	-- LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')
	-- LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')
WHERE n.nspname IN ('account','document')
  AND c.relkind = 'r'
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY 1, 2
|]

tblColumns = [str|
SELECT * FROM (SELECT n.nspname,c.relname,a.attname,a.atttypid,a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,a.atttypmod,a.attlen,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,dsc.description,t.typbasetype,t.typtype  FROM pg_catalog.pg_namespace n  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') 
WHERE a.attnum > 0 AND NOT a.attisdropped  AND n.nspname LIKE 'account' AND c.relname LIKE 'user_table') c WHERE true  ORDER BY nspname,c.relname,attnum 
|]


tblIndices2 = [str|
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE,   NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME,   CASE i.indisclustered     WHEN true THEN 1    ELSE CASE am.amname       WHEN 'hash' THEN 2      ELSE 3    END   END AS TYPE,   (i.keys).n AS ORDINAL_POSITION,   pg_catalog.pg_get_indexdef(ci.oid, (i.keys).n, false) AS COLUMN_NAME,   CASE am.amcanorder     WHEN true THEN CASE i.indoption[(i.keys).n - 1] & 1       WHEN 1 THEN 'D'       ELSE 'A'     END     ELSE NULL   END AS ASC_OR_DESC,   ci.reltuples AS CARDINALITY,   ci.relpages AS PAGES,   pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indoption,           i.indisunique, i.indisclustered,
i.indpred,           i.indexprs,           information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i     ON (ct.oid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)   JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) WHERE true  AND n.nspname = 'account' AND ct.relname = 'user_table' ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION
|]

tblIndices = [str|
select ind.indisclustered, ind.indexrelid, ind.indisprimary, cls.relname  from pg_catalog.pg_index ind, pg_catalog.pg_class tab, pg_catalog.pg_namespace sch, pg_catalog.pg_class cls  where ind.indrelid = tab.oid  and cls.oid = ind.indexrelid  and tab.relnamespace = sch.oid  and tab.relname = $1 and sch.nspname = $2
|]

tblConstraints = [str|
SELECT cons.conname, cons.conkey
FROM pg_catalog.pg_constraint cons, pg_catalog.pg_class tab, pg_catalog.pg_namespace sch 
WHERE cons.contype = 'u'  and cons.conrelid = tab.oid  and tab.relnamespace = sch.oid 
  AND tab.relname = $1 and sch.nspname = $2
|]

tblKeysx = [str|
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (i.keys).n AS KEY_SEQ, ci.relname AS PK_NAME FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN (SELECT i.indexrelid, i.indrelid, i.indisprimary,              information_schema._pg_expandarray(i.indkey) AS keys         FROM pg_catalog.pg_index i) i     ON (a.attnum = (i.keys).x AND a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = 'account' AND ct.relname = 'user_table' AND i.indisprimary  ORDER BY table_name, pk_name, key_seq
|]

tblKeys = [str|
SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, pos.n AS KEY_SEQ, CASE con.confupdtype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS UPDATE_RULE, CASE con.confdeltype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS DELETE_RULE, con.conname AS FK_NAME, pkic.relname AS PK_NAME, CASE  WHEN con.condeferrable AND con.condeferred THEN 5 WHEN con.condeferrable THEN 6 ELSE 7 END AS DEFERRABILITY  FROM  pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka,  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka,
pg_catalog.pg_constraint con,  pg_catalog.generate_series(1, 32) pos(n),  pg_catalog.pg_depend dep, pg_catalog.pg_class pkic  WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid  AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid  AND con.contype = 'f' AND con.oid = dep.objid AND pkic.oid = dep.refobjid AND pkic.relkind = 'i' AND dep.classid = 'pg_constraint'::regclass::oid AND dep.refclassid = 'pg_class'::regclass::oid  AND fkn.nspname = 'account' AND fkc.relname = 'user_table' ORDER BY pkn.nspname,pkc.relname,pos.n
|]


typeList = [str|
SELECT typname FROM pg_catalog.pg_type WHERE oid = $1

SELECT typinput='array_in'::regproc, typtype FROM pg_catalog.pg_type WHERE typname = $1

select t.oid, t.typname, s.nspname from pg_type t, pg_namespace s where t.typnamespace = s.oid
select t.typname as udtname, ct.typname as datatype, a.atttypmod as len, t.oid, a.attname as name from pg_catalog.pg_type t, pg_catalog.pg_class c, pg_catalog.pg_attribute a, pg_catalog.pg_type ct, pg_catalog.pg_namespace sch  where t.typtype = 'c' and t.typnamespace = sch.oid and t.typrelid = c.oid and sch.nspname = $1 and c.relkind = 'c' and c.oid = a.attrelid and a.atttypid = ct.oid order by t.typname, a.attnum

|]

stuff = [str|
SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  
  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  
    WHEN true THEN 
      CASE
	    WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN 
          CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  
        WHEN n.nspname = 'pg_toast' THEN 
	      CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END
      END
    WHEN false THEN 
      CASE c.relkind  
        WHEN 'r' THEN 'TABLE'  
        WHEN 'i' THEN 'INDEX'  
        WHEN 'S' THEN 'SEQUENCE'  
        WHEN 'v' THEN 'VIEW'  
        WHEN 'c' THEN 'TYPE'  
        WHEN 'f' THEN 'FOREIGN TABLE'  
        ELSE NULL
      END 
    ELSE NULL
  END AS TABLE_TYPE,
  d.description AS REMARKS 
FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)  LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')  LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')  WHERE c.relnamespace = n.oid  AND n.nspname LIKE 'account' AND (false  OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) )
ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME 
|]


