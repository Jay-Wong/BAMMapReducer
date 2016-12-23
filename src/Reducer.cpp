#include "Reducer.h"

Reducer::Reducer()
{
    memset(csFileDir, 0, sizeof(csFileDir));
    memset(csOutFileName, 0, sizeof(csOutFileName));
    strcpy(csFileDir, "../data/");
    strcpy(csOutFileName, "map.out");
    cbIsBusy = false;
}

void* Reducer::ReduceMapEntry()
{
    while (true)
    {
        char   lacCmd[128] = {0};
        char   lsFileName[128] = {0};
        char   lsFilePath[128] = {0};

        /* ---- move stat file ---- */
        sprintf(lacCmd, "mv %s*stat* ./useful/", csFileDir);
        system(lacCmd);
        
        /* ---- move map file ---- */
        memset(lacCmd, 0, sizeof(lacCmd));
        sprintf(lacCmd, "mv %s*map* ./useful/ ", csFileDir);
        system(lacCmd);
        
        /* ---- move other files ---- */
        memset(lacCmd, 0, sizeof(lacCmd));
        sprintf(lacCmd, "mv %s* ./useless/ ", csFileDir);
        system(lacCmd);
        
        /*---- remove other files ---- */
        system("rm ./useless/* ");
        
        /* ---- list map files ---- */
        system("ls ./useful/ | grep map > MapFileList.txt");

        ifstream fin("MapFileList.txt");
        while (fin.getline(lsFileName, sizeof(lsFileName)))
        {
            cbIsBusy = true;
            
            char lsCurTime[20] = {0};
            char lsLogInfo[128] = {0};
            
            /* ---- Log info ---- */
            sprintf(lsLogInfo, "Handled %s", lsFileName);
            CLogService::GetInstance()->Write("system.log", LOG_INFO, lsLogInfo);
            
            /* ---- move the file to ./tmp ---- */
            memset(lacCmd, 0, sizeof(lacCmd));
            sprintf(lacCmd, "mv ./useful/%s ./useful/handled/ ", lsFileName);
            system(lacCmd);
            
            /* ---- Handle stat file ---- */
            sprintf(lsFilePath, "./useful/handled/%s", lsFileName);
            Reduce(lsFilePath);
        }
        cbIsBusy = false;
        
        /* ---- remove useless or handled files ---- */
        system("rm ./useful/handled/*");
        system("rm MapFileList.txt");

        sleep(60);
    }
    return NULL;
}


void ConstructMapRecord(string asMapRecord, MapRecord* aoMapRecord)
{
    int    liStartPos = 0;
    int    liEndPos;
    int    liTmpPos;
    int    liTmpNum;
    time_t liTime;
    char   lacBuff[32] = {0};
    string lsTmpStr;

    liTmpPos = asMapRecord.find("orig_name:");
    liStartPos = liTmpPos + 10;
    liEndPos = asMapRecord.find(";", liStartPos);
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    strcpy(aoMapRecord->csOrigName, lsTmpStr.c_str());
    
    liTmpPos = asMapRecord.find("up_name:");
    liStartPos = liTmpPos + 8;
    liEndPos = asMapRecord.find(";", liStartPos);
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    strcpy(aoMapRecord->csUpName, lsTmpStr.c_str());
        
    liTmpPos = asMapRecord.find("dn_name:");
    liStartPos = liTmpPos + 8;
    liEndPos = asMapRecord.find(";", liStartPos);
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    strcpy(aoMapRecord->csDnName, lsTmpStr.c_str());
        
    liTmpPos = asMapRecord.find("md_time:");
    liStartPos = liTmpPos + 8;
    liEndPos = asMapRecord.find(";", liStartPos);
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    stpcpy(lacBuff, lsTmpStr.c_str());
    liTime = StrToTime(lacBuff); 
    aoMapRecord->ciMdTime = liTime;
    
    liTmpPos = asMapRecord.find("md_delay:");
    liStartPos = liTmpPos + 9;
    liEndPos = asMapRecord.find(";", liStartPos);
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    stpcpy(lacBuff, lsTmpStr.c_str());
    liTmpNum = StrToInt(lacBuff, strlen(lacBuff)); 
    aoMapRecord->ciMdDelay = liTmpNum;
    
    liTmpPos = asMapRecord.find("sheet_cnt:");
    liStartPos = liTmpPos + 10;
    liEndPos = asMapRecord.find(";", liStartPos);
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    stpcpy(lacBuff, lsTmpStr.c_str());
    liTmpNum = StrToInt(lacBuff, strlen(lacBuff)); 
    aoMapRecord->ciSheetCnt = liTmpNum;
    
    liTmpPos = asMapRecord.find("up_time:");
    liStartPos = liTmpPos + 8;
    liEndPos = asMapRecord.find(";", liStartPos);
    if (asMapRecord.size() - 1 != liEndPos)
    {
        liEndPos = asMapRecord.size();
    }
    lsTmpStr = asMapRecord.substr(liStartPos, liEndPos - liStartPos);
    stpcpy(lacBuff, lsTmpStr.c_str());
    liTime = StrToTime(lacBuff); 
    aoMapRecord->ciUpTime = liTime;
}

void Reducer::Reduce(char* apcFileName)
{
    char     lacMapRecord[256] = {0};
    string   lsMapRecord;
    string   lsLastDnName;
    
    MapRecord loMapRecord;
    vector<MapRecord> loRecordSet;

    ifstream fin(apcFileName);
    while(fin.getline(lacMapRecord, sizeof(lacMapRecord)))
    {
        lsMapRecord = string(lacMapRecord);

        ConstructMapRecord(lsMapRecord, &loMapRecord);
        
        if (loRecordSet.size() == 0)
        {
            lsLastDnName = loMapRecord.csDnName;
        }
        
        if (strcmp(loMapRecord.csDnName, lsLastDnName.c_str()) == 0)
        {
            loRecordSet.push_back(loMapRecord);
        }
        else
        {
            GenerateNewRecord(loRecordSet);
            loRecordSet.clear();
            loRecordSet.push_back(loMapRecord);
            lsLastDnName = string(loMapRecord.csDnName);
        }
    }
    if (loRecordSet.size() != 0)
    {
        GenerateNewRecord(loRecordSet);
        loRecordSet.clear();
        loRecordSet.push_back(loMapRecord);
        lsLastDnName = string(loMapRecord.csDnName);
    }
}

int Reducer::GenerateNewRecord(vector<MapRecord>& aoRecordSet)
{
    int    liSize;
    int    liStartPos;
    int    liPos;
    time_t liCurTime;
    
    Pair   loPair;
    string loUpProCode;
    string loCGCode;
    string loMiniGrpKey;
    string loTmpStr;
    
    char lsLogInfo[128] = {0};
    
    liSize = aoRecordSet.size();
    assert(liSize > 0);
    
    strcpy(loPair.csDestFileName, aoRecordSet[0].csDnName);
    
    if (KVDatabase::GetInstance()->Get(&loPair) != 0)
    {
        sprintf(lsLogInfo, "Query fail: no key %s in KVDB", loPair.csDestFileName);
        CLogService::GetInstance()->Write("system.log", LOG_INFO, lsLogInfo);
        coToReduce.push_back(aoRecordSet);
        return -1;
    }
    
    liCurTime = time(NULL);
    if ((int)(liCurTime - (loPair.coDataElem).ciCreateTime) > 86400)
    {
        sprintf(lsLogInfo, "Query fail: no key %s in KVDB", loPair.csDestFileName);
        CLogService::GetInstance()->Write("system.log", LOG_INFO, lsLogInfo);
        coToReduce.push_back(aoRecordSet);
        return -1;
    }
    
    memset(lsLogInfo, 0, sizeof(lsLogInfo));
    sprintf(lsLogInfo, "Query success: %s", loPair.csDestFileName);
    CLogService::GetInstance()->Write("system.log", LOG_INFO, lsLogInfo);
    
    /* ---- delete the index from KVDatabase ---- */
    KVDatabase::GetInstance()->Delete(&loPair);
    

    /**
     * regrouping the record set, group by "up_province_code"(extract from up_name),
     * and CG_code(extract from orig_name).
     *
     * The key format is "up_province_code|CG_code".
     */
    map<string, vector<MapRecord> > loMiniSet;
    for (int i = 0; i < liSize; ++i)
    {
        loTmpStr = string(aoRecordSet[i].csUpName);
        liPos = loTmpStr.find("CNT");
        loUpProCode = loTmpStr.substr(liPos+3, 3);
        loMiniGrpKey = loUpProCode + "|";
        
        int liStartPos = 0;
        loTmpStr = string(aoRecordSet[i].csOrigName);
        liPos = loTmpStr.find("_", liStartPos);
        loCGCode = loTmpStr.substr(liStartPos, liPos - liStartPos) + ":";
        liStartPos = liPos+1;
        
        liPos = loTmpStr.find("_", liStartPos);
        loCGCode += loTmpStr.substr(liStartPos, liPos - liStartPos) + ":";
        liStartPos = liPos+1;
        
        liPos = loTmpStr.find("_", liStartPos);
        liStartPos = liPos+1;
        
        liPos = loTmpStr.find("_", liStartPos);
        loCGCode += loTmpStr.substr(liStartPos, liPos - liStartPos);
        loMiniGrpKey += loCGCode;

        loMiniSet[loMiniGrpKey].push_back(aoRecordSet[i]);
    }
    
    /**
     * Construct format new record, and then output to file.
     */
    char     lacOutput[256] = {0};
    ofstream loOfResult(csOutFileName, ios::app); 
    map<string, vector<MapRecord> >::iterator loItr;
    for (loItr = loMiniSet.begin(); loItr != loMiniSet.end(); ++loItr)
    {
        memset(lacOutput, 0, sizeof(lacOutput));
        strftime(lacOutput, sizeof(lacOutput), "%Y%m%d%H%M%S;", localtime(&liCurTime));
        strcpy(lacOutput+strlen(lacOutput), loUpProCode.c_str());
        strcpy(lacOutput+strlen(lacOutput), ";");
        strcpy(lacOutput+strlen(lacOutput), loCGCode.c_str());
        strcpy(lacOutput+strlen(lacOutput), ";");
        strcpy(lacOutput+strlen(lacOutput), aoRecordSet[0].csDnName);
        strcpy(lacOutput+strlen(lacOutput), ";");
        
        int liAvgUpDelay = 0;
        int liTotalAvgDelay = 0;
        int liTotalCnt = 0;
        int liAvgDownDelay = 0;
        time_t liDateTime = StrToTime(loPair.coDataElem.csDatetime);
        liSize = (loItr->second).size();
        for (int i = 0; i < liSize; ++i)
        {
            liAvgUpDelay += (liDateTime - (loItr->second)[i].ciMdTime)* (loItr->second)[i].ciSheetCnt;
            liTotalAvgDelay += (loItr->second)[i].ciMdDelay * (loItr->second)[i].ciSheetCnt;
            liTotalCnt += (loItr->second)[i].ciSheetCnt;
            liAvgDownDelay += ((loItr->second)[i].ciUpTime - (loItr->second)[i].ciMdTime)* (loItr->second)[i].ciSheetCnt;
        }
        
        liAvgUpDelay /= liTotalCnt;
        liTotalAvgDelay /= liTotalCnt;
        liAvgDownDelay /= liTotalCnt;
        
        strcpy(lacOutput+strlen(lacOutput), IntToStr(liAvgUpDelay).c_str());
        strcpy(lacOutput+strlen(lacOutput), ";");
        strcpy(lacOutput+strlen(lacOutput), IntToStr(liTotalAvgDelay).c_str());
        strcpy(lacOutput+strlen(lacOutput), ";");
        strcpy(lacOutput+strlen(lacOutput), IntToStr(liTotalCnt).c_str());
        strcpy(lacOutput+strlen(lacOutput), ";");
        strcpy(lacOutput+strlen(lacOutput), IntToStr(liAvgDownDelay).c_str());
        
        loOfResult << lacOutput << endl;
    }

    return 0;
}

/**
 * @brief: if cbIsBusy is true(it means Reduce is handling map files),
 * the thread sleep 30 seconds, and continue loop.
 * After each requery, thread also sleep 30 seconds. 
 */
void Reducer::Requery()
{
    while (true)
    {
        if (cbIsBusy)
        {
            sleep(30);
            continue;
        }

        list<vector<MapRecord> >::iterator loItr = coToReduce.begin();
        time_t liCurTime;
        int    liSize = coToReduce.size();
        for (int i = 0; i < liSize && !cbIsBusy; ++i)
        {
            liCurTime = time(NULL);
            if (liCurTime - (*loItr)[0].ciFirstFailTime > 24 * HOUR_SEC)
            {
                coToReduce.erase(loItr++);
                continue;
            }
            else if (liCurTime - (*loItr)[0].ciLastFailTime < 5 * MIN_SEC)
            {
                break;
            }
            else
            {
                GenerateNewRecord(*loItr);
                coToReduce.erase(loItr++);
            }
        }
        sleep(30);
    }
}

/////////////////////////////////////////////////////////

time_t StrToTime(char* apcStrTime)
{
    tm   loTm;  
    char lacBuff[24]= {0};  
    strcpy(lacBuff, apcStrTime);  
    strptime(lacBuff, "%Y%m%d%H%M%S", &loTm);
    loTm.tm_isdst = -1;  
    return mktime(&loTm);
}

void TimeToStr(time_t aiTime, char* apcStrTime)
{
    tm   loTm;
    char lacBuff[24]= {0};  
    loTm = *localtime(&aiTime);
    strftime(lacBuff, sizeof(lacBuff), "%Y%m%d%H%M%S", &loTm);  
    strcpy(apcStrTime, lacBuff);
}

int StrToInt(char* apcBuff, int size)
{
    int liRet = 0;
    for (int i = 0; i < size; ++i)
    {
        liRet = liRet * 10 + (int)(apcBuff[i] - '0');
    }
    return liRet;
}

string IntToStr(int aiNum)
{
    string lsRet;
    if (0 == aiNum)
    {
        return "0";
    }
    
    while (aiNum)
    {
        lsRet = (char)(aiNum%10 + '0') + lsRet;
        aiNum /= 10;
    }
    
    return lsRet;
}
