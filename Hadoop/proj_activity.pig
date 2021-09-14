-- Load input file from HDFS
inputFile1 = LOAD 'hdfs:///projects/episodeIV_dialouges.txt' AS (line);
inputFile2 = LOAD 'hdfs:///projects/episodeV_dialouges.txt' AS (line);
inputFile3 = LOAD 'hdfs:///projects/episodeVI_dialouges.txt' AS (line);
prefinalfile= UNION inputFile1, inputFile2;
finalfile= UNION prefinalfile, inputFile3;
-- Tokeize each word in the file (Map)
words = FOREACH finalfile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE group as name, COUNT(words) as nline ;--ORDER grpd BY * DESC;;
--- order result based on count
order_by_data= ORDER cntd BY nline ASC;
-- Store the result in HDFS
STORE order_by_data INTO 'hdfs:///projects/results_new';

--hdfs dfs -cat /projects/results_new/part-r-00000