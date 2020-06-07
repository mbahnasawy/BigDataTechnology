records = LOAD '/home/cloudera/Desktop/Pig/InputForWC.txt' USING TextLoader AS (record:chararray);
bagOfBags = FOREACH records GENERATE TOKENIZE(record, '\t| |&|^|?|!|#|*|,|(|)|-|@') AS wordsBag;
flatBag = FOREACH bagOfBags GENERATE flatten(wordsBag) AS word;
wordsGroup = GROUP flatBag BY word;
wordsCount = FOREACH wordsGroup GENERATE group, COUNT(flatBag);
DUMP wordsCount;
STORE wordsCount INTO 'wordsCountOutput';