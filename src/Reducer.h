#ifndef _REDUCER_H_
#define _REDUCER_H_

#include "KVDatabase.h"
#include <stdlib.h>
#include <list>

typedef struct
{
    char   csOrigName[64];
    char   csUpName[64];
    char   csDnName[64];
    time_t ciMdTime;
    time_t ciMdDelay;
    int    ciSheetCnt;
    time_t ciUpTime;
    time_t ciDatetime;
    time_t ciFirstFailTime;
    time_t ciLastFailTime;
} MapRecord;

class Reducer
{
public:
    Reducer();
    
    /**
     * @brief Check the specific directory (which receive STAT files and MAP files)in cycle,
     *    and call Reduce() to handle map files.
     */
    void* ReduceMapEntry();
    
    /**
     * @brief Handle(or call "reduce") map files.
     */
    void  Reduce(char* apcFilePath);
    
    /**
     * @brief Requery KVDatabase if no map files are reducing.
     */
    void  Requery();
    
    /**
     * @brief Reduce a record set group by "dn_name", and then generate new records.
     * @param [in] aoRecordSet -- the record set to reduce 
     */
    int   GenerateNewRecord(vector<MapRecord>& aoRecordSet);

private:

    /**
     * @brief List that store the record set group by "dn_name",
     *   whose "dn_name" has no corresponding index in KVDatabase.
     *  And it will be requery in cycle.
     */
    list<vector<MapRecord> > coToReduce;
    
    /**
     * @brief Output file name.
     */
    char csOutFileName[64];
    
    /**
     * @brief Map file directory.
     */
    char csFileDir[64];
    
    /**
     * @brief If Reduce() method is handling map files, cbIsBusy is true,
     *   otherwise it is false.
     */
    bool cbIsBusy;
};

/**
 * @brief Construct a MapRecord by original map records.
 * @param [in]  asMapRecord -- original map records
 * @param [out] aoMapRecord -- the goal MapRecord object
 */
void ConstructMapRecord(string asMapRecord, MapRecord* aoMapRecord);

/**
 * @brief Trans the format time string to time_t
 */
time_t StrToTime(char* apcStrTime);

/**
 * @brief Trans the  time_t to format time string
 * @param [in]  aiTime -- time_t value to trans.
 * @param [out] apcStrTime -- the  goal fomat time string
 */
void   TimeToStr(time_t aiTime, char* apcStrTime);

/**
 * @brief Trans string to int
 */
int    StrToInt(char* apcBuff, int size);

/**
 * @brief Trans int to string
 */
string IntToStr(int aiNum);

#endif