#!/bin/bash -l
spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/carValueTag/carValueTagcollector.py > supcapLog/carValueTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/peopleValueTag/cemTagCollector.py > supcapLog/cemTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/phoneTag/cacTagCollector.py > supcapLog/cacTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/phoneTag/dossPhoneTagCollector.py > supcapLog/dossPhoneTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/sys_datastore/datastore_getbuyparts.py > supcapLog/buypartsTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/sys_datastore/datastore_getclaimtag.py > supcapLog/claimtag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/sys_datastore/datastore_getlatelytag.py > supcapLog/latelytag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/sys_datastore/datastore_getmaintag.py > supcapLog/datastoreMainTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/tm_tag/phone/tm_getPhonetag.py > supcapLog/tmPhonetag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/tm_tag/vin/tm_gettmtag.py > supcapLog/tmVintag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/tt_table/tt_tableasparquet.py > supcapLog/ttTag.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 17 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/tt_table/tt_mongo.py > supcapLog/ttMongo.log 2>&1