#ifndef _MAPPER_H_
#define _MAPPER_H_

#include "KVDatabase.h"
#include <stdlib.h> 

class CMapper
{
public:
    CMapper();
    ~CMapper();

    /**
     * @brief handle stat file, extract index and put into KVDatabase.
     */
    void  PutData(char* apcFileName);
    
    /**
     * @brief Check the specific directory (which receive STAT files and MAP files)in cycle,
     *    and call PutData() to handle stat files.
     */
    void* HandleStatEntry();
private:
    KVDatabase *cpoDatabase;
    char csFileDir[128];
};

#endif