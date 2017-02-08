cmake_minimum_required(VERSION 2.8)

set(CMAKE_DISABLE_SOURCE_CHANGES ON)
set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

project(tfs)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# We probably don't want this to run on every build.
option(COVERALLS "Generate coveralls data" OFF)
option(COVERALLS_UPLOAD "Upload the generated coveralls json" ON)

if (COVERALLS)
    include(Coveralls)
    coveralls_turn_on_coverage()
endif()

include(cotire)

add_compile_options(-Wall -Werror -pipe -fvisibility=hidden)

if (CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(-fno-strict-aliasing)
endif()

include(FindCXX11)

# Find packages.
find_package(GMP REQUIRED)
find_package(PugiXML REQUIRED)
find_package(LuaJIT)
find_package(MySQL)
find_package(PostgreSQL)
find_package(ODBC)
find_package(Threads)

option(USE_LUAJIT "Use LuaJIT" ${LUAJIT_FOUND})

if(USE_LUAJIT)
    find_package(LuaJIT REQUIRED)
    if(APPLE)
      set(CMAKE_EXE_LINKER_FLAGS "-pagezero_size 10000 -image_base 100000000")
    endif()
else()
    find_package(Lua)
endif()

find_package(Boost 1.53.0 COMPONENTS system iostreams REQUIRED)

add_subdirectory(src)
add_executable(tfs ${tfs_SRC})

if (COVERALLS)
    # Create the coveralls target.
    coveralls_setup(
        "${tfs_SRC}" # The source files.
        ${COVERALLS_UPLOAD} # If we should upload.
		)
endif()

# Add a test for the library.
enable_testing()

include_directories(${ODBC_INCLUDE_DIR} ${POSTGRESQL_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR} ${LUA_INCLUDE_DIR} ${Boost_INCLUDE_DIRS} ${PUGIXML_INCLUDE_DIR} ${GMP_INCLUDE_DIR})
target_link_libraries(tfs ${ODBC_LIBRARIES} ${POSTGRESQL_LIBRARIES} ${MYSQL_CLIENT_LIBS} ${LUA_LIBRARIES} ${Boost_LIBRARIES} ${PUGIXML_LIBRARIES} ${GMP_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

set_target_properties(tfs PROPERTIES COTIRE_CXX_PREFIX_HEADER_INIT "src/otpch.h")
set_target_properties(tfs PROPERTIES COTIRE_ADD_UNITY_BUILD FALSE)
cotire(tfs)

add_test(complicated ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/tfs)