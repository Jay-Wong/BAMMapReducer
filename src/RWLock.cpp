#include "RWLock.h"

CRWLock::CRWLock() : ciDirty(0), ciReadWait(0), ciWriteWait(0)
{
    int liRetCode;
    
    liRetCode = pthread_mutex_init(&ctMutex, NULL);
    assert(0 == liRetCode);

    liRetCode = pthread_cond_init(&ctRCond, NULL);
    assert(0 == liRetCode);

    liRetCode = pthread_cond_init(&ctWCond, NULL);
    assert(0 == liRetCode);
}

CRWLock::~CRWLock()
{
    int liRetCode;

    liRetCode = pthread_mutex_destroy(&ctMutex);
    assert(0 == liRetCode);

    liRetCode = pthread_cond_destroy(&ctRCond);
    assert(0 == liRetCode);

    liRetCode = pthread_cond_destroy(&ctWCond);
    assert(0 == liRetCode);
}

void CRWLock::RLock()
{
    int liRetCode = pthread_mutex_lock(&ctMutex);
    assert(0 == liRetCode);

    ++ciReadWait;

    while (ciDirty < 0 || ciWriteWait > 0)
    {
        pthread_cond_wait(&ctRCond, &ctMutex);
    }

    --ciReadWait;
    ++ciDirty;

    liRetCode = pthread_mutex_unlock(&ctMutex);
    assert(0 == liRetCode);
}

void CRWLock::WLock()
{
    int liRetCode;

    liRetCode = pthread_mutex_lock(&ctMutex);
    assert(0 == liRetCode);

    ++ciWriteWait;

    while (ciDirty != 0)
    {
        pthread_cond_wait(&ctWCond, &ctMutex);
    }

    --ciWriteWait;
    ciDirty = -1;

    liRetCode = pthread_mutex_unlock(&ctMutex);
    assert(0 == liRetCode);
}

void CRWLock::Unlock()
{
    int liRetCode;

    liRetCode = pthread_mutex_lock(&ctMutex);
    assert(0 == liRetCode);

    if (-1 == ciDirty)
    { // wlock
        ciDirty= 0;
    }
    else
    { // rlock
        --ciDirty;
    }

    if (0 == ciDirty)
    {
        if (ciWriteWait > 0)
        {
            liRetCode = pthread_cond_signal(&ctWCond);
            assert(0 == liRetCode);
        }
        else if (ciReadWait > 0)
        {
            liRetCode = pthread_cond_broadcast(&ctRCond);
            assert(0 == liRetCode);
        }
    }

    liRetCode = pthread_mutex_unlock(&ctMutex);
    assert(0 == liRetCode);
}
