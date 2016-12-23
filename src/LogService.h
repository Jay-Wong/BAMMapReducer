#include <cstdio>
#include <fstream>
#include <string>
#include <cstring>
#include <stdint.h>
#include "RWLock.h"

#define LOG_DEBUG (90001)
#define LOG_INFO  (90002)
#define LOG_ERROR (90003)

using namespace std;

class CLogService
{
public:
    int Write(const char* apcName, int aiLevel, const char* apcBuffer);
    static CLogService* GetInstance();

private:
    CLogService();
    static CLogService* cpoInstance;
    CRWLock coRWLock;

    // Garbage collector: free the CLogService instance.
    class CCollector
    {
    public:
        ~CCollector()
        {
            if (CLogService::cpoInstance)
                delete CLogService::cpoInstance;
        }
    };
    static CCollector coCollector;
};