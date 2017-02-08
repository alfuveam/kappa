#ifndef DATABASEODBC_H
#define DATABASEODBC_H

#include "database.h"

#include <sqltypes.h>
#include <sql.h>
#include <sqlext.h>

class ODBCResult;
using DBResult_ptrODBC = std::shared_ptr<ODBCResult>;

class DatabaseODBC : public Database
{
public:
	DatabaseODBC();
	virtual ~DatabaseODBC();
	bool executeQuery(const std::string& query);
	DBResult_ptr storeQuery(const std::string& query);
	std::string escapeString(const std::string& s) const;
	std::string escapeBlob(const char* s, uint32_t length) const;
	uint64_t getLastInsertId() { return 0; }
	std::string getClientVersion() { return "''"; }

	std::string parse(const std::string &s);

protected:
	SQLHDBC handle = SQL_NULL_HDBC;
	SQLHENV env = SQL_NULL_HENV;

	//bool beginTransaction();
	//bool rollback();
	//bool commit();
};

class ODBCResult : public DBResult
{
public:
	int64_t getNumberAny(std::string const& s) const;
	std::string getString(const std::string& s) const;
	const char* getStream(const std::string& s, uint64_t& size) const;
	bool hasNext();
	bool next();

	ODBCResult(SQLHSTMT stmt);
	virtual ~ODBCResult();
protected:

	std::map<const std::string, size_t> listNames;	

	SQLHSTMT handle = SQL_NULL_HSTMT;
	friend class DatabaseODBC;
};

#endif // DATABASEODBC_H