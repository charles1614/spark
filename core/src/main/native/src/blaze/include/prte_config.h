/* src/include/prte_config.h.  Generated from prte_config.h.in by configure.  */
/* src/include/prte_config.h.in.  Generated from configure.ac by autoheader.  */

/* -*- c -*-
 *
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * Function: - OS, CPU and compiler dependent configuration
 */

#ifndef PRTE_CONFIG_H
#define PRTE_CONFIG_H

#include "prte_config_top.h"


/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* The normal alignment of `bool', in bytes. */
#define ALIGNOF_BOOL 1

/* The normal alignment of `char', in bytes. */
#define ALIGNOF_CHAR 1

/* The normal alignment of `double', in bytes. */
#define ALIGNOF_DOUBLE 8

/* The normal alignment of `double _Complex', in bytes. */
#define ALIGNOF_DOUBLE__COMPLEX 8

/* The normal alignment of `float', in bytes. */
#define ALIGNOF_FLOAT 4

/* The normal alignment of `float _Complex', in bytes. */
#define ALIGNOF_FLOAT__COMPLEX 4

/* The normal alignment of `int', in bytes. */
#define ALIGNOF_INT 4

/* The normal alignment of `int128_t', in bytes. */
/* #undef ALIGNOF_INT128_T */

/* The normal alignment of `int16_t', in bytes. */
#define ALIGNOF_INT16_T 2

/* The normal alignment of `int32_t', in bytes. */
#define ALIGNOF_INT32_T 4

/* The normal alignment of `int64_t', in bytes. */
#define ALIGNOF_INT64_T 8

/* The normal alignment of `int8_t', in bytes. */
#define ALIGNOF_INT8_T 1

/* The normal alignment of `long', in bytes. */
#define ALIGNOF_LONG 8

/* The normal alignment of `long double', in bytes. */
#define ALIGNOF_LONG_DOUBLE 16

/* The normal alignment of `long double _Complex', in bytes. */
#define ALIGNOF_LONG_DOUBLE__COMPLEX 16

/* The normal alignment of `long long', in bytes. */
#define ALIGNOF_LONG_LONG 8

/* The normal alignment of `short', in bytes. */
#define ALIGNOF_SHORT 2

/* The normal alignment of `size_t', in bytes. */
#define ALIGNOF_SIZE_T 8

/* The normal alignment of `void *', in bytes. */
#define ALIGNOF_VOID_P 8

/* The normal alignment of `wchar_t', in bytes. */
#define ALIGNOF_WCHAR_T 4

/* The normal alignment of `__float128', in bytes. */
#define ALIGNOF___FLOAT128 16

/* defined to 1 if cray uGNI available, 0 otherwise */
/* #undef CRAY_UGNI */

/* defined to 1 if cray wlm available, 0 otherwise */
/* #undef CRAY_WLM_DETECT */

/* Define to 1 if you have the <aio.h> header file. */
#define HAVE_AIO_H 1

/* Define to 1 if you have the <alloca.h> header file. */
#define HAVE_ALLOCA_H 1

/* Define to 1 if you have the <alps/apInfo.h> header file. */
/* #undef HAVE_ALPS_APINFO_H */

/* Define to 1 if you have the <arpa/inet.h> header file. */
#define HAVE_ARPA_INET_H 1

/* Define to 1 if you have the `asprintf' function. */
#define HAVE_ASPRINTF 1

/* Define to 1 if you have the <complex.h> header file. */
#define HAVE_COMPLEX_H 1

/* Define to 1 if you have the <crt_externs.h> header file. */
/* #undef HAVE_CRT_EXTERNS_H */

/* Define to 1 if you have the `dbm_open' function. */
/* #undef HAVE_DBM_OPEN */

/* Define to 1 if you have the `dbopen' function. */
/* #undef HAVE_DBOPEN */

/* Define to 1 if you have the <db.h> header file. */
#define HAVE_DB_H 1

/* Define to 1 if you have the declaration of `AF_INET6', and to 0 if you
   don't. */
#define HAVE_DECL_AF_INET6 1

/* Define to 1 if you have the declaration of `AF_UNSPEC', and to 0 if you
   don't. */
#define HAVE_DECL_AF_UNSPEC 1

/* Define to 1 if you have the declaration of `ethtool_cmd_speed', and to 0 if
   you don't. */
#define HAVE_DECL_ETHTOOL_CMD_SPEED 1

/* Define to 1 if you have the declaration of `PF_INET6', and to 0 if you
   don't. */
#define HAVE_DECL_PF_INET6 1

/* Define to 1 if you have the declaration of `PF_UNSPEC', and to 0 if you
   don't. */
#define HAVE_DECL_PF_UNSPEC 1

/* Define to 1 if you have the declaration of `RLIMIT_AS', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_AS 1

/* Define to 1 if you have the declaration of `RLIMIT_CORE', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_CORE 1

/* Define to 1 if you have the declaration of `RLIMIT_FSIZE', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_FSIZE 1

/* Define to 1 if you have the declaration of `RLIMIT_MEMLOCK', and to 0 if
   you don't. */
#define HAVE_DECL_RLIMIT_MEMLOCK 1

/* Define to 1 if you have the declaration of `RLIMIT_NOFILE', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_NOFILE 1

/* Define to 1 if you have the declaration of `RLIMIT_NPROC', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_NPROC 1

/* Define to 1 if you have the declaration of `RLIMIT_STACK', and to 0 if you
   don't. */
#define HAVE_DECL_RLIMIT_STACK 1

/* Define to 1 if you have the declaration of `SIOCETHTOOL', and to 0 if you
   don't. */
#define HAVE_DECL_SIOCETHTOOL 1

/* Define to 1 if you have the declaration of `__func__', and to 0 if you
   don't. */
#define HAVE_DECL___FUNC__ 1

/* Define to 1 if you have the <dirent.h> header file. */
#define HAVE_DIRENT_H 1

/* Define to 1 if you have the <dlfcn.h> header file. */
#define HAVE_DLFCN_H 1

/* Define to 1 if the system has the type `double _Complex'. */
#define HAVE_DOUBLE__COMPLEX 1

/* Define to 1 if you have the <endian.h> header file. */
#define HAVE_ENDIAN_H 1

/* Define to 1 if you have the <err.h> header file. */
#define HAVE_ERR_H 1

/* Define to 1 if you have the <event.h> header file. */
#define HAVE_EVENT_H 1

/* Define to 1 if you have the <execinfo.h> header file. */
#define HAVE_EXECINFO_H 1

/* Define to 1 if you have the `execve' function. */
#define HAVE_EXECVE 1

/* Define to 1 if you have the <fcntl.h> header file. */
#define HAVE_FCNTL_H 1

/* Define to 1 if the system has the type `float _Complex'. */
#define HAVE_FLOAT__COMPLEX 1

/* Define to 1 if you have the `fork' function. */
#define HAVE_FORK 1

/* Define to 1 if you have the `getpwuid' function. */
#define HAVE_GETPWUID 1

/* Define to 1 if you have the <grp.h> header file. */
#define HAVE_GRP_H 1

/* Define to 1 if you have the <hostLib.h> header file. */
/* #undef HAVE_HOSTLIB_H */

/* Define to 1 if you have the <hwloc.h> header file. */
#define HAVE_HWLOC_H 1

/* Define to 1 if you have the <ieee754.h> header file. */
#define HAVE_IEEE754_H 1

/* Define to 1 if you have the <ifaddrs.h> header file. */
#define HAVE_IFADDRS_H 1

/* Define to 1 if the system has the type `int128_t'. */
/* #undef HAVE_INT128_T */

/* Define to 1 if the system has the type `int16_t'. */
#define HAVE_INT16_T 1

/* Define to 1 if the system has the type `int32_t'. */
#define HAVE_INT32_T 1

/* Define to 1 if the system has the type `int64_t'. */
#define HAVE_INT64_T 1

/* Define to 1 if the system has the type `int8_t'. */
#define HAVE_INT8_T 1

/* Define to 1 if the system has the type `intptr_t'. */
#define HAVE_INTPTR_T 1

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the <ioLib.h> header file. */
/* #undef HAVE_IOLIB_H */

/* Define to 1 if you have the `isatty' function. */
#define HAVE_ISATTY 1

/* Define to 1 if you have the `event_core' library (-levent_core). */
#define HAVE_LIBEVENT_CORE 1

/* Define to 1 if you have the `event_pthreads' library (-levent_pthreads). */
#define HAVE_LIBEVENT_PTHREADS 1

/* Define to 1 if you have the <libgen.h> header file. */
#define HAVE_LIBGEN_H 1

/* Define to 1 if you have the <libutil.h> header file. */
/* #undef HAVE_LIBUTIL_H */

/* Define to 1 if you have the <linux/ethtool.h> header file. */
#define HAVE_LINUX_ETHTOOL_H 1

/* Define to 1 if you have the <linux/sockios.h> header file. */
#define HAVE_LINUX_SOCKIOS_H 1

/* Define to 1 if the system has the type `long double'. */
#define HAVE_LONG_DOUBLE 1

/* Define to 1 if the system has the type `long double _Complex'. */
#define HAVE_LONG_DOUBLE__COMPLEX 1

/* Define to 1 if the system has the type `long long'. */
#define HAVE_LONG_LONG 1

/* Define to 1 if you have the <lsf/lsbatch.h> header file. */
/* #undef HAVE_LSF_LSBATCH_H */

/* Define to 1 if you have the <lsf/lsf.h> header file. */
/* #undef HAVE_LSF_LSF_H */

/* Define to 1 if you have the <ltprtedl.h> header file. */
/* #undef HAVE_LTPRTEDL_H */

/* Define to 1 if you have the <malloc.h> header file. */
#define HAVE_MALLOC_H 1

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mkfifo' function. */
#define HAVE_MKFIFO 1

/* Define to 1 if you have the `mmap' function. */
#define HAVE_MMAP 1

/* Define to 1 if you have the <mntent.h> header file. */
#define HAVE_MNTENT_H 1

/* Define to 1 if the system has the type `mode_t'. */
#define HAVE_MODE_T 1

/* Define to 1 if you have the <ndbm.h> header file. */
#define HAVE_NDBM_H 1

/* Define to 1 if you have the <netdb.h> header file. */
#define HAVE_NETDB_H 1

/* Define to 1 if you have the <netinet/in.h> header file. */
#define HAVE_NETINET_IN_H 1

/* Define to 1 if you have the <netinet/tcp.h> header file. */
#define HAVE_NETINET_TCP_H 1

/* Define to 1 if you have the <netlink/version.h> header file. */
#define HAVE_NETLINK_VERSION_H 1

/* Define to 1 if you have the <net/if.h> header file. */
#define HAVE_NET_IF_H 1

/* Define to 1 if you have the <net/uio.h> header file. */
/* #undef HAVE_NET_UIO_H */

/* Define to 1 if you have the `openpty' function. */
#define HAVE_OPENPTY 1

/* Define to 1 if you have the <paths.h> header file. */
#define HAVE_PATHS_H 1

/* Define to 1 if you have the `pipe' function. */
#define HAVE_PIPE 1

/* Define to 1 if you have the <poll.h> header file. */
#define HAVE_POLL_H 1

/* Define to 1 if you have the `posix_memalign' function. */
#define HAVE_POSIX_MEMALIGN 1

/* Define to 1 if you have the `printstack' function. */
/* #undef HAVE_PRINTSTACK */

/* Define to 1 if you have the `pthread_condattr_setpshared' function. */
#define HAVE_PTHREAD_CONDATTR_SETPSHARED 1

/* Define to 1 if you have the <pthread.h> header file. */
#define HAVE_PTHREAD_H 1

/* Define to 1 if you have the `pthread_mutexattr_setpshared' function. */
#define HAVE_PTHREAD_MUTEXATTR_SETPSHARED 1

/* Define to 1 if the system has the type `ptrdiff_t'. */
#define HAVE_PTRDIFF_T 1

/* Define to 1 if you have the `ptsname' function. */
#define HAVE_PTSNAME 1

/* Define to 1 if you have the <pty.h> header file. */
#define HAVE_PTY_H 1

/* Define to 1 if you have the <pwd.h> header file. */
#define HAVE_PWD_H 1

/* Define to 1 if you have the `regcmp' function. */
/* #undef HAVE_REGCMP */

/* Define to 1 if you have the `regexec' function. */
#define HAVE_REGEXEC 1

/* Define to 1 if you have the <regex.h> header file. */
#define HAVE_REGEX_H 1

/* Define to 1 if you have the `regfree' function. */
#define HAVE_REGFREE 1

/* Define to 1 if you have the <sched.h> header file. */
#define HAVE_SCHED_H 1

/* Define to 1 if you have the `setenv' function. */
#define HAVE_SETENV 1

/* Define to 1 if you have the `setpgid' function. */
#define HAVE_SETPGID 1

/* Define to 1 if you have the `setsid' function. */
#define HAVE_SETSID 1

/* Define to 1 if you have the <shlwapi.h> header file. */
/* #undef HAVE_SHLWAPI_H */

/* Define to 1 if `si_band' is a member of `siginfo_t'. */
#define HAVE_SIGINFO_T_SI_BAND 1

/* Define to 1 if `si_fd' is a member of `siginfo_t'. */
#define HAVE_SIGINFO_T_SI_FD 1

/* Define to 1 if you have the `snprintf' function. */
#define HAVE_SNPRINTF 1

/* Define to 1 if you have the `socketpair' function. */
#define HAVE_SOCKETPAIR 1

/* Define to 1 if the system has the type `socklen_t'. */
#define HAVE_SOCKLEN_T 1

/* Define to 1 if you have the <sockLib.h> header file. */
/* #undef HAVE_SOCKLIB_H */

/* Define to 1 if the system has the type `ssize_t'. */
#define HAVE_SSIZE_T 1

/* Define to 1 if you have the `statfs' function. */
#define HAVE_STATFS 1

/* Define to 1 if you have the `statvfs' function. */
#define HAVE_STATVFS 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `strncpy_s' function. */
/* #undef HAVE_STRNCPY_S */

/* Define to 1 if you have the <stropts.h> header file. */
/* #undef HAVE_STROPTS_H */

/* Define to 1 if you have the `strsignal' function. */
#define HAVE_STRSIGNAL 1

/* Define to 1 if `d_type' is a member of `struct dirent'. */
#define HAVE_STRUCT_DIRENT_D_TYPE 1

/* Define to 1 if the system has the type `struct ethtool_cmd'. */
#define HAVE_STRUCT_ETHTOOL_CMD 1

/* Define to 1 if `speed_hi' is a member of `struct ethtool_cmd'. */
#define HAVE_STRUCT_ETHTOOL_CMD_SPEED_HI 1

/* Define to 1 if the system has the type `struct ifreq'. */
#define HAVE_STRUCT_IFREQ 1

/* Define to 1 if `ifr_hwaddr' is a member of `struct ifreq'. */
#define HAVE_STRUCT_IFREQ_IFR_HWADDR 1

/* Define to 1 if `ifr_mtu' is a member of `struct ifreq'. */
#define HAVE_STRUCT_IFREQ_IFR_MTU 1

/* Define to 1 if the system has the type `struct sockaddr_in'. */
#define HAVE_STRUCT_SOCKADDR_IN 1

/* Define to 1 if the system has the type `struct sockaddr_in6'. */
#define HAVE_STRUCT_SOCKADDR_IN6 1

/* Define to 1 if `sa_len' is a member of `struct sockaddr'. */
/* #undef HAVE_STRUCT_SOCKADDR_SA_LEN */

/* Define to 1 if the system has the type `struct sockaddr_storage'. */
#define HAVE_STRUCT_SOCKADDR_STORAGE 1

/* Define to 1 if `f_fstypename' is a member of `struct statfs'. */
/* #undef HAVE_STRUCT_STATFS_F_FSTYPENAME */

/* Define to 1 if `f_type' is a member of `struct statfs'. */
#define HAVE_STRUCT_STATFS_F_TYPE 1

/* Define to 1 if `f_basetype' is a member of `struct statvfs'. */
/* #undef HAVE_STRUCT_STATVFS_F_BASETYPE */

/* Define to 1 if `f_fstypename' is a member of `struct statvfs'. */
/* #undef HAVE_STRUCT_STATVFS_F_FSTYPENAME */

/* Define to 1 if you have the `sysconf' function. */
#define HAVE_SYSCONF 1

/* Define to 1 if you have the `syslog' function. */
#define HAVE_SYSLOG 1

/* Define to 1 if you have the <syslog.h> header file. */
#define HAVE_SYSLOG_H 1

/* Define to 1 if you have the <sys/fcntl.h> header file. */
#define HAVE_SYS_FCNTL_H 1

/* Define to 1 if you have the <sys/ioctl.h> header file. */
#define HAVE_SYS_IOCTL_H 1

/* Define to 1 if you have the <sys/ipc.h> header file. */
#define HAVE_SYS_IPC_H 1

/* Define to 1 if you have the <sys/mman.h> header file. */
#define HAVE_SYS_MMAN_H 1

/* Define to 1 if you have the <sys/mount.h> header file. */
#define HAVE_SYS_MOUNT_H 1

/* Define to 1 if you have the <sys/param.h> header file. */
#define HAVE_SYS_PARAM_H 1

/* Whether or not we have the ptrace header */
#define HAVE_SYS_PTRACE_H 1

/* Define to 1 if you have the <sys/queue.h> header file. */
#define HAVE_SYS_QUEUE_H 1

/* Define to 1 if you have the <sys/resource.h> header file. */
#define HAVE_SYS_RESOURCE_H 1

/* Define to 1 if you have the <sys/select.h> header file. */
#define HAVE_SYS_SELECT_H 1

/* Define to 1 if you have the <sys/shm.h> header file. */
#define HAVE_SYS_SHM_H 1

/* Define to 1 if you have the <sys/socket.h> header file. */
#define HAVE_SYS_SOCKET_H 1

/* Define to 1 if you have the <sys/sockio.h> header file. */
/* #undef HAVE_SYS_SOCKIO_H */

/* Define to 1 if you have the <sys/statfs.h> header file. */
#define HAVE_SYS_STATFS_H 1

/* Define to 1 if you have the <sys/statvfs.h> header file. */
#define HAVE_SYS_STATVFS_H 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/synch.h> header file. */
/* #undef HAVE_SYS_SYNCH_H */

/* Define to 1 if you have the <sys/sysctl.h> header file. */
/* #undef HAVE_SYS_SYSCTL_H */

/* Define to 1 if you have the <sys/time.h> header file. */
#define HAVE_SYS_TIME_H 1

/* Define to 1 if you have the <sys/tree.h> header file. */
/* #undef HAVE_SYS_TREE_H */

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <sys/uio.h> header file. */
#define HAVE_SYS_UIO_H 1

/* Define to 1 if you have the <sys/un.h> header file. */
#define HAVE_SYS_UN_H 1

/* Define to 1 if you have the <sys/utsname.h> header file. */
#define HAVE_SYS_UTSNAME_H 1

/* Define to 1 if you have the <sys/vfs.h> header file. */
#define HAVE_SYS_VFS_H 1

/* Define to 1 if you have the <sys/wait.h> header file. */
#define HAVE_SYS_WAIT_H 1

/* Define to 1 if you have the `tcgetpgrp' function. */
#define HAVE_TCGETPGRP 1

/* Define to 1 if you have the <termios.h> header file. */
#define HAVE_TERMIOS_H 1

/* Define to 1 if you have the <tm.h> header file. */
/* #undef HAVE_TM_H */

/* Define to 1 if you have the <ucontext.h> header file. */
#define HAVE_UCONTEXT_H 1

/* Define to 1 if the system has the type `uint128_t'. */
/* #undef HAVE_UINT128_T */

/* Define to 1 if the system has the type `uint16_t'. */
#define HAVE_UINT16_T 1

/* Define to 1 if the system has the type `uint32_t'. */
#define HAVE_UINT32_T 1

/* Define to 1 if the system has the type `uint64_t'. */
#define HAVE_UINT64_T 1

/* Define to 1 if the system has the type `uint8_t'. */
#define HAVE_UINT8_T 1

/* Define to 1 if the system has the type `uintptr_t'. */
#define HAVE_UINTPTR_T 1

/* Define to 1 if you have the <ulimit.h> header file. */
#define HAVE_ULIMIT_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* whether unix byteswap routines -- htonl, htons, nothl, ntohs -- are
   available */
#define HAVE_UNIX_BYTESWAP 1

/* Define to 1 if you have the `usleep' function. */
#define HAVE_USLEEP 1

/* Define to 1 if you have the <util.h> header file. */
/* #undef HAVE_UTIL_H */

/* Define to 1 if you have the <utmp.h> header file. */
#define HAVE_UTMP_H 1

/* Define to 1 if you have the `vasprintf' function. */
#define HAVE_VASPRINTF 1

/* Define to 1 if you have the `vsnprintf' function. */
#define HAVE_VSNPRINTF 1

/* Define to 1 if you have the `vsyslog' function. */
#define HAVE_VSYSLOG 1

/* Define to 1 if you have the `waitpid' function. */
#define HAVE_WAITPID 1

/* Define to 1 if you have the `_NSGetEnviron' function. */
/* #undef HAVE__NSGETENVIRON */

/* Define to 1 if the system has the type `__float128'. */
#define HAVE___FLOAT128 1

/* Define to 1 if the system has the type `__int128'. */
#define HAVE___INT128 1

/* Define to 1 if you have the `__malloc_initialize_hook' function. */
/* #undef HAVE___MALLOC_INITIALIZE_HOOK */

/* Define to the sub-directory in which libtool stores uninstalled libraries.
   */
#define LT_OBJDIR ".libs/"

/* Define to 1 if your C compiler doesn't accept -c and -o together. */
/* #undef NO_MINUS_C_MINUS_O */

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "https://github.com/openpmix/prte/"

/* Define to the full name of this package. */
#define PACKAGE_NAME "prte"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "prte gitclone"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "prte"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "gitclone"

/* PRTE architecture string */
#define PRTE_ARCH "x86_64-pc-linux-gnu"

/* Use C11 style atomics */
#define PRTE_ATOMIC_C11 1

/* Use GCC builtin style atomics */
#define PRTE_ATOMIC_GCC_BUILTIN 0

/* whether building on x86_64 platform */
#define PRTE_ATOMIC_X86_64 1

/* The compiler $lower which PMIx was built with */
#define PRTE_BUILD_PLATFORM_COMPILER_FAMILYID 1

/* The compiler $lower which PRTE was built with */
#define PRTE_BUILD_PLATFORM_COMPILER_FAMILYNAME GNU

/* The compiler $lower which PMIx was built with */
#define PRTE_BUILD_PLATFORM_COMPILER_VERSION 721152

/* The compiler $lower which PMIx was built with */
#define PRTE_BUILD_PLATFORM_COMPILER_VERSION_STR 11.1.0

/* OMPI underlying C compiler */
#define PRTE_CC "gcc"

/* Use static const char[] strings for C files */
#define PRTE_CC_USE_CONST_CHAR_IDENT 0

/* Use #ident strings for C files */
#define PRTE_CC_USE_IDENT 1

/* Use #pragma comment for C files */
#define PRTE_CC_USE_PRAGMA_COMMENT 

/* Use #pragma ident strings for C files */
#define PRTE_CC_USE_PRAGMA_IDENT 0

/* Specific ps command to use in prte-clean */
#define PRTE_CLEAN_PS_CMD "ps -A -o fname,pid,uid"

/* Capture the configure cmd line */
#define PRTE_CONFIGURE_CLI " \'--disable-option-checking\' \'--prefix=/opt/ompi\' \'--enable-prte-ft\' \'--with-proxy-version-string=5.0.0a1\' \'--with-proxy-package-name=Open MPI\' \'--with-proxy-bugreport=https://www.open-mpi.org/community/help/\' \'--enable-prte-prefix-by-default\' \'--with-prte-extra-lib=\' \'--with-prte-extra-ltlib=\' \'--enable-debug\' \'--enable-debug\' \'--with-hwloc=/opt/deps/hwloc\' \'--with-pmix=/opt/deps/openpmix\' \'--enable-mpi-java\' \'--enable-mem-debug\' \'--enable-mem-profile\' \'--with-prrte=/opt/deps/prrte\' \'--cache-file=/dev/null\' \'--srcdir=.\'"

/* Date when PMIx was built */
#define PRTE_CONFIGURE_DATE "Thu Jun 24 03:24:10 UTC 2021"

/* Hostname where PMIx was built */
#define PRTE_CONFIGURE_HOST "lenovo"

/* User who built PMIx */
#define PRTE_CONFIGURE_USER "xialb"

/* Whether C compiler supports atomic convenience variables in stdatomic.h */
#define PRTE_C_HAVE_ATOMIC_CONV_VAR 1

/* Whether C compiler supports __builtin_clz */
#define PRTE_C_HAVE_BUILTIN_CLZ 0

/* Whether C compiler supports __builtin_expect */
#define PRTE_C_HAVE_BUILTIN_EXPECT 0

/* Whether C compiler supports __builtin_prefetch */
#define PRTE_C_HAVE_BUILTIN_PREFETCH 0

/* Whether C compiler supports symbol visibility or not */
#define PRTE_C_HAVE_VISIBILITY 1

/* Whether C compiler supports __Atomic keyword */
#define PRTE_C_HAVE__ATOMIC 1

/* Whether C compiler supports __Generic keyword */
#define PRTE_C_HAVE__GENERIC 1

/* Whether C compiler supports _Static_assert keyword */
#define PRTE_C_HAVE__STATIC_ASSERT 1

/* Whether C compiler supports __Thread_local */
#define PRTE_C_HAVE__THREAD_LOCAL 1

/* Whether C compiler supports __thread */
#define PRTE_C_HAVE___THREAD 1

/* Command to detach from process being traced */
#define PRTE_DETACH PT_DETACH

/* Whether we have lt_dladvise or not */
#define PRTE_DL_LIBLTDL_HAVE_LT_DLADVISE 0

/* Whether we want developer-level debugging code or not */
#define PRTE_ENABLE_DEBUG 1

/* Whether we want to enable dlopen support */
#define PRTE_ENABLE_DLOPEN_SUPPORT 1

/* Enable PRRTE fault tolerance support (default: disabled) */
#define PRTE_ENABLE_FT 1

/* Disable getpwuid support (default: enabled) */
#define PRTE_ENABLE_GETPWUID 1

/* Enable IPv6 support, but only if the underlying system supports it */
#define PRTE_ENABLE_IPV6 0

/* Whether or not we will build manpages */
#define PRTE_ENABLE_MAN_PAGES 1

/* Whether we should enable thread support within the PRTE code base */
#define PRTE_ENABLE_MULTI_THREADS 1

/* Whether user wants PTY support or not */
#define PRTE_ENABLE_PTY_SUPPORT 1

/* Location of event2/thread.h */
#define PRTE_EVENT2_THREAD_HEADER <event2/thread.h>

/* Location of event.h */
#define PRTE_EVENT_HEADER <event.h>

/* Greek - alpha, beta, etc - release number of PMIx Reference Run-Time
   Environment */
#define PRTE_GREEK_VERSION "a1"

/* Whether or not we have apple */
#define PRTE_HAVE_APPLE 0

/* Whether your compiler has __attribute__ or not */
#define PRTE_HAVE_ATTRIBUTE 1

/* Whether your compiler has __attribute__ aligned or not */
#define PRTE_HAVE_ATTRIBUTE_ALIGNED 1

/* Whether your compiler has __attribute__ always_inline or not */
#define PRTE_HAVE_ATTRIBUTE_ALWAYS_INLINE 1

/* Whether your compiler has __attribute__ cold or not */
#define PRTE_HAVE_ATTRIBUTE_COLD 1

/* Whether your compiler has __attribute__ const or not */
#define PRTE_HAVE_ATTRIBUTE_CONST 1

/* Whether your compiler has __attribute__ deprecated or not */
#define PRTE_HAVE_ATTRIBUTE_DEPRECATED 1

/* Whether your compiler has __attribute__ deprecated with optional argument
   */
#define PRTE_HAVE_ATTRIBUTE_DEPRECATED_ARGUMENT 1

/* Whether your compiler has __attribute__ destructor or not */
#define PRTE_HAVE_ATTRIBUTE_DESTRUCTOR 1

/* Whether your compiler has __attribute__ extension or not */
#define PRTE_HAVE_ATTRIBUTE_EXTENSION 1

/* Whether your compiler has __attribute__ format or not */
#define PRTE_HAVE_ATTRIBUTE_FORMAT 1

/* Whether your compiler has __attribute__ format and it works on function
   pointers */
#define PRTE_HAVE_ATTRIBUTE_FORMAT_FUNCPTR 1

/* Whether your compiler has __attribute__ hot or not */
#define PRTE_HAVE_ATTRIBUTE_HOT 1

/* Whether your compiler has __attribute__ malloc or not */
#define PRTE_HAVE_ATTRIBUTE_MALLOC 1

/* Whether your compiler has __attribute__ may_alias or not */
#define PRTE_HAVE_ATTRIBUTE_MAY_ALIAS 1

/* Whether your compiler has __attribute__ noinline or not */
#define PRTE_HAVE_ATTRIBUTE_NOINLINE 1

/* Whether your compiler has __attribute__ nonnull or not */
#define PRTE_HAVE_ATTRIBUTE_NONNULL 1

/* Whether your compiler has __attribute__ noreturn or not */
#define PRTE_HAVE_ATTRIBUTE_NORETURN 1

/* Whether your compiler has __attribute__ noreturn and it works on function
   pointers */
#define PRTE_HAVE_ATTRIBUTE_NORETURN_FUNCPTR 1

/* Whether your compiler has __attribute__ no_instrument_function or not */
#define PRTE_HAVE_ATTRIBUTE_NO_INSTRUMENT_FUNCTION 1

/* Whether your compiler has __attribute__ optnone or not */
#define PRTE_HAVE_ATTRIBUTE_OPTNONE 0

/* Whether your compiler has __attribute__ packed or not */
#define PRTE_HAVE_ATTRIBUTE_PACKED 1

/* Whether your compiler has __attribute__ pure or not */
#define PRTE_HAVE_ATTRIBUTE_PURE 1

/* Whether your compiler has __attribute__ sentinel or not */
#define PRTE_HAVE_ATTRIBUTE_SENTINEL 1

/* Whether your compiler has __attribute__ unused or not */
#define PRTE_HAVE_ATTRIBUTE_UNUSED 1

/* Whether your compiler has __attribute__ visibility or not */
#define PRTE_HAVE_ATTRIBUTE_VISIBILITY 1

/* Whether your compiler has __attribute__ warn unused result or not */
#define PRTE_HAVE_ATTRIBUTE_WARN_UNUSED_RESULT 1

/* Whether your compiler has __attribute__ weak alias or not */
#define PRTE_HAVE_ATTRIBUTE_WEAK_ALIAS 1

/* whether backtrace_execinfo is found and available */
#define PRTE_HAVE_BACKTRACE_EXECINFO 1

/* whether qsort is broken or not */
#define PRTE_HAVE_BROKEN_QSORT 0

/* whether ceil is found and available */
#define PRTE_HAVE_CEIL 1

/* Whether we have Clang __c11 atomic functions */
/* #undef PRTE_HAVE_CLANG_BUILTIN_ATOMIC_C11_FUNC */

/* whether clock_gettime is found and available */
#define PRTE_HAVE_CLOCK_GETTIME 1

/* defined to 1 if cray alps env, 0 otherwise */
#define PRTE_HAVE_CRAY_ALPS 0

/* whether dirname is found and available */
#define PRTE_HAVE_DIRNAME 1

/* Whether the PRTE DL framework is functional or not */
#define PRTE_HAVE_DL_SUPPORT 1

/* whether gethostbyname is found and available */
#define PRTE_HAVE_GETHOSTBYNAME 1

/* Whether or not we have hwloc support */
#define PRTE_HAVE_HWLOC 1

/* Whether or not hwloc_topology_dup is available */
#define PRTE_HAVE_HWLOC_TOPOLOGY_DUP 1

/* Whether we are building against libev */
#define PRTE_HAVE_LIBEV 0

/* Whether we are building against libevent */
#define PRTE_HAVE_LIBEVENT 1

/* Does ptrace have the Linux signature */
#define PRTE_HAVE_LINUX_PTRACE 0

/* whether openpty is found and available */
#define PRTE_HAVE_OPENPTY 1

/* If PTHREADS implementation supports PTHREAD_MUTEX_ERRORCHECK */
#define PRTE_HAVE_PTHREAD_MUTEX_ERRORCHECK 1

/* If PTHREADS implementation supports PTHREAD_MUTEX_ERRORCHECK_NP */
#define PRTE_HAVE_PTHREAD_MUTEX_ERRORCHECK_NP 1

/* Whether we have SA_RESTART in <signal.h> or not */
#define PRTE_HAVE_SA_RESTART 1

/* whether sched_yield is found and available */
#define PRTE_HAVE_SCHED_YIELD 1

/* whether shm_open_rt is found and available */
#define PRTE_HAVE_SHM_OPEN_RT 1

/* whether socket is found and available */
#define PRTE_HAVE_SOCKET 1

/* Whether or not we have solaris */
#define PRTE_HAVE_SOLARIS 0

/* Whether or not we have stop-on-exec support */
#define PRTE_HAVE_STOP_ON_EXEC 1

/* Whether we have __va_copy or not */
#define PRTE_HAVE_UNDERSCORE_VA_COPY 1

/* Whether we have va_copy or not */
#define PRTE_HAVE_VA_COPY 1

/* whether yp_all_nsl is found and available */
#define PRTE_HAVE_YP_ALL_NSL 1

/* Define to 1 ifyou have the declaration of _SC_NPROCESSORS_ONLN, and to 0
   otherwise */
#define PRTE_HAVE__SC_NPROCESSORS_ONLN 1

/* Location of hwloc.h */
#define PRTE_HWLOC_HEADER <hwloc.h>

/* Whether or not the hwloc header was given to us */
#define PRTE_HWLOC_HEADER_GIVEN 0

/* ident string for Open MPI */
#define PRTE_IDENT_STRING "2.0.0a1"

/* Major release number of PMIx Reference Run-Time Environment */
#define PRTE_MAJOR_VERSION 2

/* MCA cmd line identifier */
#define PRTE_MCA_CMD_LINE_ID "mca"

/* MCA prefix string for envars */
#define PRTE_MCA_PREFIX "PRTE_MCA_"

/* Minor release number of PMIx Reference Run-Time Environment */
#define PRTE_MINOR_VERSION 0

/* package/branding string for Open MPI */
#define PRTE_PACKAGE_STRING "Open MPI xialb@lenovo Distribution"

/* PMIx header to use */
#define PRTE_PMIX_HEADER <pmix.h>

/* Whether or not the PMIx header was explicitly passed */
#define PRTE_PMIX_HEADER_GIVEN 0

/* Bugreport string to be returned by prte when in proxy mode */
#define PRTE_PROXY_BUGREPORT "https://www.open-mpi.org/community/help/"

/* Package name to be returned by prte when in proxy mode */
#define PRTE_PROXY_PACKAGE_NAME "Open MPI"

/* Version string to be returned by prte when in proxy mode */
#define PRTE_PROXY_VERSION_STRING "5.0.0a1"

/* type to use for ptrdiff_t */
#define PRTE_PTRDIFF_TYPE ptrdiff_t

/* Release date of PMIx Reference Run-Time Environment */
#define PRTE_RELEASE_DATE "Nov 24, 2018"

/* Release release number of PMIx Reference Run-Time Environment */
#define PRTE_RELEASE_VERSION 0

/* The repository version PMIx Reference Run-Time Environment */
#define PRTE_REPO_REV "dev-31238-g73d50c0282"

/* Default value for mca_base_component_show_load_errors MCA variable */
#define PRTE_SHOW_LOAD_ERRORS_DEFAULT 1

/* Tarball filename version string of PMIx Reference Run-Time Environment */
#define PRTE_TARBALL_VERSION "gitclone"

/* Command for declaring that process expects to be traced by parent */
#define PRTE_TRACEME PT_TRACE_ME

/* Complete release number of PMIx Reference Run-Time Environment */
#define PRTE_VERSION "0"

/* Enable per-user config files */
#define PRTE_WANT_HOME_CONFIG_FILES 1

/* if want pretty-print stack trace feature */
#define PRTE_WANT_PRETTY_PRINT_STACKTRACE 1

/* Whether we want prte to effect "--prefix $prefix" by default */
#define PRTE_WANT_PRTE_PREFIX_BY_DEFAULT 1

/* The size of `atomic_int', as computed by sizeof. */
#define SIZEOF_ATOMIC_INT 4

/* The size of `atomic_llong', as computed by sizeof. */
#define SIZEOF_ATOMIC_LLONG 8

/* The size of `atomic_long', as computed by sizeof. */
#define SIZEOF_ATOMIC_LONG 8

/* The size of `atomic_short', as computed by sizeof. */
#define SIZEOF_ATOMIC_SHORT 2

/* The size of `char', as computed by sizeof. */
#define SIZEOF_CHAR 1

/* The size of `double', as computed by sizeof. */
#define SIZEOF_DOUBLE 8

/* The size of `double _Complex', as computed by sizeof. */
#define SIZEOF_DOUBLE__COMPLEX 16

/* The size of `float', as computed by sizeof. */
#define SIZEOF_FLOAT 4

/* The size of `float _Complex', as computed by sizeof. */
#define SIZEOF_FLOAT__COMPLEX 8

/* The size of `int', as computed by sizeof. */
#define SIZEOF_INT 4

/* The size of `long', as computed by sizeof. */
#define SIZEOF_LONG 8

/* The size of `long double', as computed by sizeof. */
#define SIZEOF_LONG_DOUBLE 16

/* The size of `long double _Complex', as computed by sizeof. */
#define SIZEOF_LONG_DOUBLE__COMPLEX 32

/* The size of `long long', as computed by sizeof. */
#define SIZEOF_LONG_LONG 8

/* The size of `pid_t', as computed by sizeof. */
#define SIZEOF_PID_T 4

/* The size of `ptrdiff_t', as computed by sizeof. */
#define SIZEOF_PTRDIFF_T 8

/* The size of `short', as computed by sizeof. */
#define SIZEOF_SHORT 2

/* The size of `size_t', as computed by sizeof. */
#define SIZEOF_SIZE_T 8

/* The size of `ssize_t', as computed by sizeof. */
#define SIZEOF_SSIZE_T 8

/* The size of `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 8

/* The size of `wchar_t', as computed by sizeof. */
#define SIZEOF_WCHAR_T 4

/* The size of `_Bool', as computed by sizeof. */
#define SIZEOF__BOOL 1

/* The size of `__float128', as computed by sizeof. */
#define SIZEOF___FLOAT128 16

/* defined to 1 if slurm cray env, 0 otherwise */
#define SLURM_CRAY_ENV 0

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Enable extensions on AIX 3, Interix.  */
#ifndef _ALL_SOURCE
# define _ALL_SOURCE 1
#endif
/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
/* Enable threading extensions on Solaris.  */
#ifndef _POSIX_PTHREAD_SEMANTICS
# define _POSIX_PTHREAD_SEMANTICS 1
#endif
/* Enable extensions on HP NonStop.  */
#ifndef _TANDEM_SOURCE
# define _TANDEM_SOURCE 1
#endif
/* Enable general extensions on Solaris.  */
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1
#endif


/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* Whether the wrapper compilers add rpath flags by default */
#define WRAPPER_RPATH_SUPPORT "runpath"

/* Define to 1 if `lex' declares `yytext' as a `char *' by default, not a
   `char[]'. */
#define YYTEXT_POINTER 1

/* Enable GNU extensions on systems that have them.  */
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif

/* Define to 1 if on MINIX. */
/* #undef _MINIX */

/* Define to 2 if the system does not provide POSIX.1 features except with
   this defined. */
/* #undef _POSIX_1_SOURCE */

/* Define to 1 if you need to in order for `stat' and other things to work. */
/* #undef _POSIX_SOURCE */


#include "prte_config_bottom.h"
#endif /* PRTE_CONFIG_H */

