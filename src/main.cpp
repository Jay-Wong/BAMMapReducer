#include "KVDatabase.h"
#include "Mapper.h"
#include "Reducer.h"
#include <pthread.h>
#include <unistd.h>

void* KVDBEntry(void*)
{
    KVDatabase* lpoDB = KVDatabase::GetInstance();
    lpoDB->Init();
    lpoDB->CheckLiveEntry();
    return NULL;
}

void* MapperEntry(void*)
{
    sleep(1);
    CMapper loMapper;
    loMapper.Init();
    loMapper.HandleStatEntry();
    return NULL;
}

void* ReducerEntry(void* apvParam)
{
    /**
     * There is no data in KVDatabase in cold boot,
     * so wait Mapper load some data.
     */
    sleep(2);
    ((Reducer*)apvParam)->Init();
    ((Reducer*)apvParam)->ReduceMapEntry();
    return NULL;
}

void* RequeryEntry(void* apvParam)
{
    sleep(3);
    ((Reducer*)apvParam)->Requery();
    return NULL;
}

int main()
{
    pthread_t liPid;
    Reducer loReducer;
    
    system("mkdir useful");
    system("mkdir useless");
    system("mkdir useful/handled");
    
    if (pthread_create(&liPid, NULL, KVDBEntry, NULL) == 0)
    {
        pthread_detach(liPid);
    }
    
    if (pthread_create(&liPid, NULL, MapperEntry, NULL) == 0)
    {
        pthread_detach(liPid);
    }
    
    if (pthread_create(&liPid, NULL, ReducerEntry, &loReducer) == 0)
    {
        pthread_detach(liPid);
    }
    
    if (pthread_create(&liPid, NULL, RequeryEntry, &loReducer) == 0)
    {
        pthread_detach(liPid);
    }
    
    while(true)
    {
        sleep(80000);
    }
    return 0;
}