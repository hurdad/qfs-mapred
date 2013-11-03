dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(qfs-mapred, 1.0)

AC_CHECK_FUNCS([iswprint]) 
AC_CHECK_HEADERS([fcntl.h])
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_FUNC_MALLOC
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

AC_CHECK_LIB([boost_program_options], [main], [],[
         echo "boost_program_options library is required for this program"
         exit -1])

AC_CHECK_LIB([boost_thread], [main], [],[
         echo "boost_thread library is required for this program"
         exit -1])

AC_CHECK_LIB([gearman], [main], [],[
         echo "gearman library is required for this program"
         exit -1])

AC_CHECK_LIB([pthread], [main], [],[
         echo "pthread library is required for this program"
         exit -1])


AC_CANONICAL_SYSTEM
AM_INIT_AUTOMAKE()

AC_PROG_CXX

AC_CONFIG_FILES(Makefile src/Makefile)
AC_OUTPUT

