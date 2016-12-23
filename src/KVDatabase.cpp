#include "KVDatabase.h"

KVDatabase* KVDatabase::cpoInstance = NULL;
KVDatabase::CCollector KVDatabase::coCollector;

KVDatabase::KVDatabase() {}

int KVDatabase::Init()
{
    char   lacGoal[32]  = {0};
    char   lacBuff[128] = {0};
    string lsSubstr;
    ifstream fin("../config/config.ini");
    while (fin.getline(lacBuff, sizeof(lacBuff)))
    {
        for (int i = 0; i < strlen(lacBuff); ++i)
        {
            if ('\r' == lacBuff[i] || '\n' == lacBuff[i])
            {
                lacBuff[i] = 0;
            }
        }
        lsSubstr = string(lacBuff);
        if (lsSubstr.find("IndexValidSecond") == 0)
        {
            memcpy(lacGoal, lacBuff+17, strlen(lacBuff)-17);
            ciMaxValidSec = StrToInt(lacGoal, strlen(lacGoal));
            return 0;
        }
    }
    return -1;
}

KVDatabase* KVDatabase::GetInstance()
{
    if (cpoInstance == NULL)
        cpoInstance = new KVDatabase();
    return cpoInstance;
}

int KVDatabase::Put(Pair* apoPair)
{
    if (apoPair == NULL)
    {
        return -1;
    }
    coRWLock.WLock();
    string loKey = string(apoPair->csDestFileName);
    KVList[loKey] = apoPair->coDataElem;
    coRWLock.Unlock();
    return 0;
}

int KVDatabase::Get(Pair* apoPair)
{
    if (apoPair == NULL)
    {
        return -1;
    }
    string loKey = string(apoPair->csDestFileName);
    map<string, DataElem>::iterator loItr = KVList.find(loKey);
    if (loItr == KVList.end())
    {
        return -1;
    }
    apoPair->coDataElem = KVList[loKey];
    return 0;    
}

int KVDatabase::Delete(Pair* apoPair)
{
    map<string, DataElem>::iterator loItr = KVList.begin();
    while (loItr != KVList.end())
    {
        if (strcmp(loItr->first.c_str(), apoPair->csDestFileName) == 0)
        {
            coRWLock.WLock();
            KVList.erase(loItr++);
            coRWLock.Unlock();
            return 0;
        }
        else
        {
            ++loItr;
        }
    }
    return -1;
}

void* KVDatabase::CheckLiveEntry()
{
    while (true)
    {
        CheckLive();
        sleep(2 * HOUR_SEC);
    }
    return NULL;
}

void KVDatabase::CheckLive()
{
    map<string, DataElem>::iterator loItr = KVList.begin();
    time_t liStoreTime, liNowTime;
    while (loItr != KVList.end())
    {
        liStoreTime = loItr->second.ciCreateTime;
        liNowTime = time(NULL);
        if (liNowTime - liStoreTime >= ciMaxValidSec)
        {
            coRWLock.WLock();
            KVList.erase(loItr++);
            coRWLock.Unlock();
        }
        else
        {
            ++loItr;
        }
    }
}

void KVDatabase::Print()
{
    char lacLogInfo[128] = {0};
    sprintf(lacLogInfo, "Index number: %d", KVList.size());
    map<string, DataElem>::iterator itr = KVList.begin();
    while (itr != KVList.end())
    {
        memset(lacLogInfo, 0, sizeof(lacLogInfo));
        sprintf(lacLogInfo, "key: %s, value: %s", itr->first.c_str(), itr->second.csDatetime);
        ++itr;
    }
}

//////////////////////////////////////////

int StrToInt(const char* apcBuff, int size)
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
