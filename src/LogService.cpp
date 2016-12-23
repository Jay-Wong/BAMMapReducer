#include "LogService.h"

CLogService* CLogService::cpoInstance = NULL;
CLogService::CCollector CLogService::coCollector;

CLogService::CLogService() {}

int CLogService::Write(const char* apcName, int aiLevel, const char* apcBuffer)
{
    size_t auLen = strlen(apcBuffer);
    time_t ltTime = time(0);
    char   lacBuff[256] = {0};
    char   lsLevel[16] = {0};
    ofstream loFile;
    coRWLock.WLock();
    loFile.open(apcName, ios::app);
    if (!loFile.is_open())
        return -1;
    strftime(lacBuff,sizeof(lacBuff),"# %Y-%m-%d %H:%M:%S #",localtime(&ltTime));
    switch(aiLevel)
    {
        case LOG_DEBUG:
            strncpy(lsLevel, "DEBUG", sizeof(lsLevel));
            break;
        case LOG_INFO:
            strncpy(lsLevel, " INFO", sizeof(lsLevel));
            break;
        case LOG_ERROR:
            strncpy(lsLevel, "ERROR", sizeof(lsLevel));
            break;
        default:
            strncpy(lsLevel, "NOTYPE", sizeof(lsLevel));
            break;
    }
    sprintf(lacBuff+strlen(lacBuff), "@%s: ", lsLevel);
    strncpy(lacBuff+strlen(lacBuff), apcBuffer, auLen);
    sprintf(lacBuff+strlen(lacBuff), "\r\n");
    loFile.write(lacBuff, strlen(lacBuff));
    loFile.close();
    coRWLock.Unlock();
    return 0;
}

CLogService* CLogService::GetInstance()
{
    if (cpoInstance == NULL)
        cpoInstance = new CLogService();
    return cpoInstance;
}