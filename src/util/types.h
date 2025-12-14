#ifndef NOSTR_UTIL_TYPES_H_
#define NOSTR_UTIL_TYPES_H_

#ifndef _INT8_T
#define _INT8_T
typedef char int8_t;
#endif

#ifndef _UINT8_T
#define _UINT8_T
typedef unsigned char uint8_t;
#endif

#ifndef _INT16_T
#define _INT16_T
typedef short int16_t;
#endif

#ifndef _UINT16_T
#define _UINT16_T
typedef unsigned short uint16_t;
#endif

#ifndef _INT32_T
#define _INT32_T
typedef int int32_t;
#endif

#ifndef _UINT32_T
#define _UINT32_T
typedef unsigned int uint32_t;
#endif

#ifndef _INT64_T
#define _INT64_T
typedef long long int64_t;
#endif

#ifndef _UINT64_T
#define _UINT64_T
typedef unsigned long long uint64_t;
#endif

#ifndef _SIZE_T
#define _SIZE_T
typedef uint64_t size_t;
#endif

#ifndef _SSIZE_T
#define _SSIZE_T
typedef int64_t ssize_t;
#endif

#ifndef _PID_T
#define _PID_T
typedef int32_t pid_t;
#endif

#ifndef _UID_T
#define _UID_T
typedef int32_t uid_t;
#endif

#ifndef _CLOCK_T
#define _CLOCK_T
typedef uint64_t clock_t;
#endif

#ifndef bool
#define bool int32_t
#endif

#ifndef true
#define true 1
#endif

#ifndef false
#define false 0
#endif

#ifndef NULL
#define NULL ((void*)0)
#endif

#ifndef STDOUT_FILENO
#define STDOUT_FILENO 1  // Standard output.
#endif

#ifndef STDERR_FILENO
#define STDERR_FILENO 2  // Standard error output.
#endif

#ifndef thread_local
#if __STDC_VERSION__ >= 201112L && !defined(__STDC_NO_THREADS__)
#define thread_local _Thread_local
#else
#define thread_local __thread
#endif
#endif

#ifndef ALIGN_UP_8
#define ALIGN_UP_8(size) ((size + 7) & ~(size_t)(7))
#endif

#endif
