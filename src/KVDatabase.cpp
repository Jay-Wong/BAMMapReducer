#include "KVDatabase.h"

KVDatabase* KVDatabase::cpoInstance = NULL;
KVDatabase::CCollector KVDatabase::coCollector;

KVDatabase::KVDatabase() {}

int KVDatabase::Init()
{
    ciMaxValidSec = DAY_SEC;
    return 0;
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
    coRWLock.WLock();
    map<string, DataElem>::iterator loItr = KVList.begin();
    while (loItr != KVList.end())
    {
        if (strcmp(loItr->first.c_str(), apoPair->csDestFileName) == 0)
        {
            KVList.erase(loItr++);
            return 0;
        }
        else
        {
            ++loItr;
        }
    }
    coRWLock.Unlock();
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
