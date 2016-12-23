#ifndef _KVDATABASE_H_
#define _KVDATABASE_H_

#include <map>
#include <string>
#include <stdio.h>
#include <iostream>
#include <assert.h>
#include <vector>
#include <map>
#include <unistd.h>
#include "LogService.h"

#define DAY_SEC  (86400)
#define HOUR_SEC (3600)
#define MIN_SEC  (60)

using namespace std;

typedef struct
{
    char   csDatetime[64];
    time_t ciCreateTime;
} DataElem;

typedef struct
{
    char     csDestFileName[64];
    DataElem coDataElem;
} Pair;

class KVDatabase
{
public:
    int   Init();
    int   Put(Pair* apoPair);
    int   Get(Pair* apoPair);
    int   Delete(Pair* apoPair);
    void  CheckLive();
    void* CheckLiveEntry();
    static KVDatabase* GetInstance();
    void Print();

private:
    KVDatabase();
    static  KVDatabase* cpoInstance;
    time_t  ciMaxValidSec;
    CRWLock coRWLock;
    map<string, DataElem> KVList;

    class CCollector
    {
    public:
        ~CCollector()
        {
            if (KVDatabase::cpoInstance)
                delete KVDatabase::cpoInstance;
        }
    };
    static CCollector coCollector;
};


/**
 * @brief Trans string to int
 */
int    StrToInt(const char* apcBuff, int size);

/**
 * @brief Trans int to string
 */
string IntToStr(int aiNum);

#endif