FILE_DATE=`date  +%Y%m%d`
FILE_HOUR_DATE=`date +%Y%m%d%H`
CRAWL_DATA_DIR=data/crawl-data
LOG_DIR=log

TODO_SPIDER=(itunesNewFreeApplication) 

if [ ! -d ${CRAWL_DATA_DIR} ]; then
    mkdir -p ${CRAWL_DATA_DIR}
fi
if [ ! -d ${LOG_DIR} ]; then
    mkdir -p ${LOG_DIR}
fi

for SPIDER in ${TODO_SPIDER[@]}; do
    scrapy crawl ${SPIDER} -o ${CRAWL_DATA_DIR}/${SPIDER}-${FILE_HOUR_DATE}.dat --logfile=${LOG_DIR}/${SPIDER}-${FILE_DATE}.log --loglevel=INFO
    if [ $? -eq 0 ]; then
	echo "${SPIDER}-${FILE_HOUR_DATE}.dat" >> ${CRAWL_DATA_DIR}/.index.tmp
	mv ${CRAWL_DATA_DIR}/.index.tmp ${CRAWL_DATA_DIR}/${SPIDER}.index
    fi
done


