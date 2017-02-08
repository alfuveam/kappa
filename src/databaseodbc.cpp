#include "otpch.h"
#include "databaseodbc.h"
#include <boost/algorithm/string/replace.hpp>


DatabaseODBC::DatabaseODBC()
{

	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (!SQL_SUCCEEDED(retcode)){
		std::cout << "Failed to allocate ODBC enviroment handle." << std::endl;
		return;
	}

	retcode = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "Failed to set ODBC environment." << std::endl;
		SQLFreeHandle(SQL_HANDLE_ENV, env);
		return;
	}

	retcode = SQLAllocHandle(SQL_HANDLE_DBC, env, &handle);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "Failed to allocate ODBC connection handle." << std::endl;
		return;
	}

	retcode = SQLSetConnectAttr(handle, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)5, 0);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "Failed to set ODBC connection timeout." << std::endl;
		SQLFreeHandle(SQL_HANDLE_DBC, handle);
		return;
	}
	retcode = SQLConnect(handle, (SQLCHAR*)g_config.getString(ConfigManager::SQL_DB).c_str(), SQL_NTS, (SQLCHAR*)g_config.getString(ConfigManager::SQL_USER).c_str(), SQL_NTS, (SQLCHAR*)g_config.getString(ConfigManager::SQL_PASS).c_str(), SQL_NTS);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "Failed to connect to ODBC via DSN: " << g_config.getString(ConfigManager::SQL_DB) << " (user " << g_config.getString(ConfigManager::SQL_USER) << ")" << std::endl;
		SQLFreeHandle(SQL_HANDLE_DBC, handle);
		return;
	}

	m_connected = true;
}


DatabaseODBC::~DatabaseODBC()
{
	if (m_connected) {
		SQLDisconnect(handle);
		SQLFreeHandle(SQL_HANDLE_DBC, handle);
		handle = NULL;
		m_connected = false;
	}

	SQLFreeHandle(SQL_HANDLE_ENV, env);
}

bool DatabaseODBC::executeQuery(const std::string & query)
{
	databaseLock.lock();
	//std::string _query = (std::string)query;
	std::string _query = parse(query);
	//boost::replace_all(_query, "`", "");

	SQLHSTMT stmt;

	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, handle, &stmt);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "Failed to allocate ODBC statement." << std::endl;
		return false;
	}

	//retcode = SQLExecDirect(stmt, (SQLCHAR*)_query.c_str(), _query.length());
	retcode = SQLExecDirect(stmt, (SQLCHAR*)_query.c_str(), _query.length());
	SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	//std::cout << "Monstra executeQuery: " << _query << std::endl;

	if (!SQL_SUCCEEDED(retcode)) {
		//SQLCHAR sqlstate[6];
		//std::cout << "SQLExecDirect(): " << _query << ": ODBC ERROR." << SQLError(env, handle, stmt, (SQLCHAR*)_query.c_str(), 0, (SQLCHAR*)a, sizeof(a), (SQLSMALLINT*)512) << std::endl;
		std::cout << "SQLExecDirect() executeQuery: " << _query << ": ODBC ERROR." << std::endl;
		//SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, sqlstate, NULL, NULL, NULL, NULL);
		//std::cout << "erro: " << SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, sqlstate, NULL, NULL, NULL, NULL) << std::endl;
		return false;
	}
	databaseLock.unlock();
	return true;
}

DBResult_ptr DatabaseODBC::storeQuery(const std::string & query)
{
	databaseLock.lock();
	SQLHSTMT stmt;	
	//std::string _query = (std::string)query;
	std::string _query = parse(query);
	//boost::replace_all(_query, "`", "");
	
	SQLRETURN retcode = SQLAllocHandle(SQL_HANDLE_STMT, handle, &stmt);
	if (!SQL_SUCCEEDED(retcode)) {
		std::cout << "Failed to allocate ODBC SQLHSTMT statement." << std::endl;
		return NULL;
	}

	//retcode = SQLExecDirect(stmt, (SQLCHAR*)_query.c_str(), _query.length());
	retcode = SQLExecDirect(stmt, (SQLCHAR*)_query.c_str(), _query.length());
	//std::cout << "Monstra storeQuery: " << _query << std::endl;
	
	if (!SQL_SUCCEEDED(retcode)) {
		//SQLCHAR sqlstate[6];
		std::cout << "SQLExecDirect() storeQuery: " << query << ": ODBC ERROR." << std::endl;	
		//SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, sqlstate, NULL, NULL, NULL, NULL);
		//std::cout << "erro: " << SQLGetDiagRec(SQL_HANDLE_DBC, handle, 1, sqlstate, NULL, NULL, NULL, NULL) << std::endl;
		return NULL;
	}
	
	databaseLock.unlock();
	DBResult_ptrODBC result = std::make_shared<ODBCResult>(stmt);
	if (!result->next()) {
		return nullptr;
	}
	return result;

}

std::string DatabaseODBC::escapeString(const std::string & s) const
{
	return escapeBlob(s.c_str(), s.length());
}

std::string DatabaseODBC::escapeBlob(const char * s, uint32_t length) const
{
	std::string buf = "'";

	for (uint32_t i = 0; i < length; ++i) {
		switch (s[i]) {
		case '\'':
			buf += "\'\'";
			break;

		case '\0':
			buf += "\\0";
			break;

		case '\\':
			buf += "\\\\";
			break;

		case '\r':
			buf += "\\r";
			break;

		case '\n':
			buf += "\\n";
			break;

		default:
			buf += s[i];
		}
	}

	buf += "'";
	return buf;
}

std::string DatabaseODBC::parse(const std::string &s)
{
	std::string query = "";

	query.reserve(s.size());
	bool inString = false;
	uint8_t ch;
	for (uint32_t a = 0; a < s.length(); a++) {
		ch = s[a];

		if (ch == '\'') {
			if (inString && s[a + 1] != '\'')
				inString = false;
			else
				inString = true;
		}

		if (ch == '`' && !inString)
			ch = '"';

		query += ch;
	}

	return query;
}

//bool DatabaseODBC::beginTransaction()
//{
//	return executeQuery("BEGIN");
//}

//bool DatabaseODBC::rollback()
//{
//	SQLRETURN retcode = SQLEndTran(SQL_HANDLE_DBC, handle, SQL_ROLLBACK);
//	if (!SQL_SUCCEEDED(retcode)) {
//		std::cout << "Error - SQLEndTran(SQL_ROLLBACK)" << std::endl;
//		return false;
//	}
//	return true;
//}
//
//bool DatabaseODBC::commit()
//{
//	SQLRETURN retcode = SQLEndTran(SQL_HANDLE_DBC, handle, SQL_COMMIT);
//	if (!SQL_SUCCEEDED(retcode)) {
//		std::cout << "Error - SQLEndTran(SQL_COMMIT)" << std::endl;		
//		return false;
//	}
//	return true;
//}

int64_t ODBCResult::getNumberAny(std::string const & s) const
{
	auto it = listNames.find(s);
	if (it != listNames.end()) {
		int64_t value;
		SQLRETURN retcode = SQLGetData(handle, it->second, SQL_C_SBIGINT, &value, 0, NULL);

		if (SQL_SUCCEEDED(retcode)) {
			return value;
		} else {
			std::cout << "Error during getNumberAny(" << s << ")." << std::endl;
		}
	}	
	return 0; // Failed
}

std::string ODBCResult::getString(const std::string & s) const
{
	auto it = listNames.find(s);
	if (it != listNames.end()) {
		char* value = new char[1024];
		SQLRETURN retcode = SQLGetData(handle, it->second, SQL_C_CHAR, value, 1024, NULL);
		// this null is because table accounts tuple secret, DEFAULT NULL
		if (retcode == SQL_NULL_DATA) {
			return std::string("");
			//strcpy(value, "NULL");
		}
		
		if (SQL_SUCCEEDED(retcode)) {
			
		std::string buff = std::string(value);
		return buff;
		}
		else {
			std::cout << "Error during getString(" << s << "). - " << value << std::endl;
		}
	}
	return std::string(""); // Failed
}

const char * ODBCResult::getStream(const std::string & s, uint64_t & size) const
{
	auto it = listNames.find(s);
	if (it != listNames.end()) {
		char* value = new char[s.length() * 2 + 1];
		SQLRETURN retcode = SQLGetData(handle, it->second, SQL_C_BINARY, value, s.length() * 2 + 1, (SQLLEN*)&size);
		// this null is because table accounts tuple secret, DEFAULT NULL
		if (SQL_SUCCEEDED(retcode)) {
			return value;
		}
		else {
			std::cout << "Error during getStream(" << s << "). - " << value << std::endl;
		}
	}
	return NULL; // Failed
}

bool ODBCResult::hasNext(){
	return ODBCResult::next();
}

bool ODBCResult::next()
{
	SQLRETURN retcode = SQLFetch(handle);
	if (!SQL_SUCCEEDED(retcode)) {
		return false;
	}
	return true;
}

ODBCResult::ODBCResult(SQLHSTMT stmt)
{
	handle = stmt;

	int16_t numCols;
	SQLNumResultCols(handle, &numCols);

	for (int32_t i = 1; i <= numCols; ++i) {
		char* name = new char[129];
		SQLDescribeCol(handle, i, (SQLCHAR*)name, 129, NULL, NULL, NULL, NULL, NULL);
		listNames[name] = i;
	}
}

ODBCResult::~ODBCResult()
{
	SQLFreeHandle(SQL_HANDLE_STMT, handle);
}

//----------------------------------------
//#include <iostream>
//using namespace std;
//
//#include <stdio.h>
//#include <string.h>
//#include <stdlib.h>
//
//// #define OTL_ODBC_UNIX // uncomment this line if UnixODBC is used
//#define OTL_ODBC_ALTERNATE_RPC
//#if !defined(_WIN32) && !defined(_WIN64)
//#define OTL_ODBC
//#else 
//#define OTL_ODBC_POSTGRESQL // required with PG ODBC on Windows
//#endif
//#include <otlv4.h> // include the OTL 4.0 header file
//
//otl_connect db; // connect object
//
//void insert()
//// insert rows into table
//{
//	otl_stream o(50, // PostgreSQL 8.1 and higher, the buffer can be > 1
//		"insert into test_tab values(:f1<int>,:f2<char[31]>)",
//		// SQL statement
//		db // connect object
//	);
//	char tmp[32];
//
//	for (int i = 1; i <= 100; ++i) {
//		sprintf(tmp, "Name%d", i);
//		o << i << tmp;
//	}
//}
//void update(const int af1)
//// insert rows into table
//{
//	otl_stream o(1, // PostgreSQL 8.1 and higher, buffer size can be >1
//		"UPDATE test_tab "
//		"   SET f2=:f2<char[31]> "
//		" WHERE f1=:f1<int>",
//		// UPDATE statement
//		db // connect object
//	);
//	o << "Name changed" << af1;
//	o << otl_null() << af1 + 1; // set f2 to NULL
//
//}
//void select(const int af1)
//{
//	otl_stream i(50, // On SELECT, the buffer size can be > 1
//		"select * from test_tab where f1>=:f11<int> and f1<=:f12<int>*2",
//		// SELECT statement
//		db // connect object
//	);
//	// create select stream
//
//	int f1;
//	char f2[31];
//
//#if (defined(_MSC_VER) && _MSC_VER>=1600) || defined(OTL_CPP_11_ON)
//	// C++11 (or higher) compiler
//#if defined(OTL_ANSI_CPP_11_VARIADIC_TEMPLATES)
//	otl_write_row(i, af1, af1); // Writing input values into the stream
//#else
//	// when variadic template functions are not supported by the C++
//	// compiler, OTL provides nonvariadic versions of the same template
//	// functions in the range of [1..15] parameters
//	otl_write_row(i, af1, af1); // Writing input values into the stream
//								// the old way (operators >>() / <<()) is available as always:
//								// i<<af1<<af1; // Writing input values into the stream
//#endif
//	for (auto& it : i) {
//		it >> f1;
//		cout << "f1=" << f1 << ", f2=";
//		it >> f2;
//		if (it.is_null())
//			cout << "NULL";
//		else
//			cout << f2;
//		cout << endl;
//	}
//#else
//	// C++98/03 compiler
//	i << af1 << af1; // Writing input values into the stream
//	while (!i.eof()) { // while not end-of-data
//		i >> f1;
//		cout << "f1=" << f1 << ", f2=";
//		i >> f2;
//		if (i.is_null())
//			cout << "NULL";
//		else
//			cout << f2;
//		cout << endl;
//	}
//#endif
//
//}
//
//DatabaseODBC::DatabaseODBC()
//{
//	otl_connect::otl_initialize(); // initialize ODBC environment
//	try {
//
//		db.rlogon("scott/tiger@postgresql");
//
//		otl_cursor::direct_exec
//		(
//			db,
//			"drop table test_tab",
//			otl_exception::disabled // disable OTL exceptions
//		); // drop table
//
//		db.commit();
//
//		otl_cursor::direct_exec
//		(
//			db,
//			"create table test_tab(f1 int, f2 varchar(30))"
//		);  // create table
//
//		db.commit();
//
//		insert(); // insert records into the table
//		update(10); // update records in the table
//		select(8); // select records from the table
//
//	}
//
//	catch (otl_exception& p) { // intercept OTL exceptions
//		cerr << p.msg << endl; // print out error message
//		cerr << p.stm_text << endl; // print out SQL that caused the error
//		cerr << p.sqlstate << endl; // print out SQLSTATE message
//		cerr << p.var_info << endl; // print out the variable that caused the error
//	}
//
//	db.logoff(); // disconnect from ODBC
//
//				 //return 0;
//
//}