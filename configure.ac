dnl Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(qfs_mapred, 1.0)

AC_CHECK_LIB([boost_program_options], [main], [],[
         echo "boost_program_options library is required for this program"
         exit -1])

AC_CHECK_LIB([boost_thread], [main], [],[
         echo "boost_thread library is required for this program"
         exit -1])

AC_CHECK_LIB([gearman], [main], [],[
         echo "gearman library is required for this program"
         exit -1])


AC_CANONICAL_SYSTEM
AM_INIT_AUTOMAKE()

AC_PROG_CXX

AC_CONFIG_FILES(Makefile src/Makefile)
AC_OUTPUT

