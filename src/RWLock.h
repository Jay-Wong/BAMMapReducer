#include <pthread.h>
#include <assert.h>

class CRWLock
{
public:
    CRWLock();
    ~CRWLock();
    void RLock();
    void WLock();
    void Unlock();

private:
    pthread_mutex_t ctMutex;
    pthread_cond_t  ctRCond;
    pthread_cond_t  ctWCond;
    int             ciDirty;
    int             ciReadWait;
    int             ciWriteWait;
};