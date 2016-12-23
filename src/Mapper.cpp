#include "Mapper.h"

CMapper::CMapper()
{
    memset(csFileDir, 0, sizeof(csFileDir));
    strcpy(csFileDir, "../data/");
    cpoDatabase = KVDatabase::GetInstance();
}

CMapper::~CMapper() {}

void* CMapper::HandleStatEntry()
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
        sprintf(lacCmd, "mv %s*map* ./useful/", csFileDir);
        system(lacCmd);
        
        /* ---- move other file ---- */
        memset(lacCmd, 0, sizeof(lacCmd));
        sprintf(lacCmd, "mv %s* ./useless/", csFileDir);
        system(lacCmd);
        
        /*---- remove other files ---- */
        //system("rm ./useless/*");
        
        /* ---- list stat files ---- */
        system("ls ./useful/ | grep stat > fileList.txt");

        ifstream fin("fileList.txt");
        while (fin.getline(lsFileName, sizeof(lsFileName)))
        {
            char lsCurTime[20] = {0};
            char lsLogInfo[128] = {0};
            
            /* ---- Log ---- */
            sprintf(lsLogInfo, "Handled %s", lsFileName);
            CLogService::GetInstance()->Write("system.log", LOG_INFO, lsLogInfo);
            
            /* ---- move the file to ./tmp ---- */
            memset(lacCmd, 0, sizeof(lacCmd));
            sprintf(lacCmd, "mv ./useful/%s ./useful/handled/", lsFileName);
            system(lacCmd);
            
            /* ---- Handle stat file ---- */
            sprintf(lsFilePath, "./useful/handled/%s", lsFileName);
            PutData(lsFilePath);
        }
        system("rm ./useful/handled/*");
        system("rm fileList.txt");
        sleep(0.5 * MIN_SEC);
    }
    return NULL;
}

void CMapper::PutData(char* apcFileName)
{
    char     lacStatRecord[256] = {0};
    string   lsStatRecord;
    string   lsTmpStr;
    
    int      liStartPos = 0;
    int      liEndPos;
    int      liTmpPos;
    
    Pair     loPair;
    DataElem loDataElem;
    memset(&loPair, 0, sizeof(loPair));
    memset(&loDataElem, 0, sizeof(loDataElem));

    ifstream fin(apcFileName);
    while(fin.getline(lacStatRecord, sizeof(lacStatRecord)))
    {
        lsStatRecord = string(lacStatRecord);
        liTmpPos = lsStatRecord.find("destfilename:");
        liStartPos = liTmpPos + 13;
        liEndPos = lsStatRecord.find(";", liStartPos);
        lsTmpStr = lsStatRecord.substr(liStartPos, liEndPos - liStartPos);
        strcpy(loPair.csDestFileName, lsTmpStr.c_str());
        
        liTmpPos = lsStatRecord.find("datetime:");
        liStartPos = liTmpPos + 9;
        liEndPos = lsStatRecord.find(";", liStartPos);
        lsTmpStr = lsStatRecord.substr(liStartPos, liEndPos - liStartPos);
        strcpy(loDataElem.csDatetime, lsTmpStr.c_str());
        loDataElem.ciCreateTime = time(NULL);
        loPair.coDataElem = loDataElem;
        cpoDatabase->Put(&loPair);
    }
}