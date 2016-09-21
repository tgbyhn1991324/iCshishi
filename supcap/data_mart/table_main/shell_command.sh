#!/bin/bash -l
# spark-submit --master yarn-client --executor-memory 17G --num-executors 17 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/tt_table/tt_mongoCopy.py > supcapLog/ttMongoCopy.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 4 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDoSSDolConfigTableCollector.py > supcapLog/ConfigTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmPhoneRIDTableCollector.py > supcapLog/PhoneRIDTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.1 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmCarDmTableA.py > supcapLog/CarTableA.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.1 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmCarDmTableB.py > supcapLog/CarTableB.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.1 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmCarDtTable.py > supcapLog/CarDtTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.1 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDatastoreGetRepairDmTable.py > supcapLog/RepairDmTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.1 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDatastoreGetBalanceDmTable.py > supcapLog/BalanceDmTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDatastoreGetRepairSalePartDmTable.py > supcapLog/RepairSalePartDmTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDatastoreGetClaimOrderDmTable.py > supcapLog/ClaimOrderDmTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.1 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmGetRepairBalanceDtTable.py > supcapLog/RepairBalanceDtTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDatastoreGetRepairSalePartDtTable.py  > supcapLog/RepairSalePartDtTable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/data_mart/table_dev/DmDatastoreGetClaimOrderDtTable.py > supcapLog/ClaimOrderDtTable.log 2>&1

spark-submit --master yarn-client --executor-memory 15G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/superid/key_key_table/key_midtableget.py > supcapLog/key_midtableget.log 2>&1

spark-submit --master yarn-client --executor-memory 30G --num-executors 10 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/superid/key_key_table/key_getfinaltable.py > supcapLog/key_finaltable.log 2>&1

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3  ~/supcap/src/com/mathartsys/supcap/tagFromMart/autoUpdate/carTypeMapUpdate.py > supcapLog/carTypeMapUpdate.log 2>&1

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

spark-submit --master yarn-client --executor-memory 17G --num-executors 15 --executor-cores 5 --conf spark.Shuffle.consolidateFiles=true --conf spark.storage.memoryFraction=0.3 --py-files ~/supcap/deploy/supcap.zip ~/supcap/src/com/mathartsys/supcap/tagFromMart/tm_tag/md5.py > supcapLog/md5.log 2>&1
