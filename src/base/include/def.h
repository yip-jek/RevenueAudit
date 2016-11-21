#pragma once

namespace base
{

// Log error code
#define LG_FILE_PATH_EMPTY                   (-1000001)
#define LG_FILE_PATH_INVALID                 (-1000002)
#define LG_INIT_FAIL                         (-1000003)
#define LG_OPEN_FILE_FAIL                    (-1000004)

// Config error code
#define CFG_FILE_INVALID                     (-1001001)
#define CFG_OPEN_FILE_FAIL                   (-1001002)
#define CFG_UNREGISTER_ITEM                  (-1001003)
#define CFG_ITEM_NOT_FOUND                   (-1001004)
#define CFG_VALUE_INVALID                    (-1001005)

// BaseDB2 error code
#define BDB_CONNECT_FAILED                   (-1002001)
#define BDB_DISCONNECT_FAILED                (-1002002)
#define BDB_BEGIN_FAILED                     (-1002003)
#define BDB_COMMIT_FAILED                    (-1002004)
#define BDB_ROLLBACK_FAILED                  (-1002005)

// BaseJHive error code
#define BJH_INIT_FAILED                      (-1003001)
#define BJH_CREATE_JVM_FAILED                (-1003002)
#define BJH_GET_CLASSPATH_FAILED             (-1003003)
#define BJH_CONNECT_FAILED                   (-1003004)
#define BJH_EXECUTE_SQL_FAILED               (-1003005)
#define BJH_FETCH_DATA_FAILED                (-1003006)

// AutoDisconnect error code
#define AUTODIS_UNABLE_CONNECT               (-1004001)

}	// namespace base

