SET ECHO ON
spool outputfileteam8.txt
col primarytitle format a15
--For the years 2014 to 2020,In the Action,Drama genre, the top five rated movies with votes>100000 are as follows
select t.primarytitle,r.averagerating from imdb00.TITLE_BASICS t INNER JOIN imdb00.TITLE_RATINGS r ON r.tconst=t.tconst where t.genres like '%Action%Drama%' and to_number(t.startyear)>=2014 and t.endyear IN(select endyear from imdb00.TITLE_BASICS where endyear!='\N') and to_number(t.endyear)<=2020 and r.tconst IN(select tconst from imdb00.TITLE_RATINGS group by tconst,averagerating having sum(numvotes)>=100000) order by r.averagerating desc fetch first 5 rows only;
EXPLAIN PLAN FOR select t.primarytitle,r.averagerating from imdb00.TITLE_BASICS t INNER JOIN imdb00.TITLE_RATINGS r ON r.tconst=t.tconst where t.genres like '%Action%Drama%' and to_number(t.startyear)>=2014 and t.endyear IN(select endyear from imdb00.TITLE_BASICS where endyear!='\N') and to_number(t.endyear)<=2020 and r.tconst IN(select tconst from imdb00.TITLE_RATINGS group by tconst,averagerating having sum(numvotes)>=100000) order by r.averagerating desc fetch first 5 rows only;
SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY());
spool OFF