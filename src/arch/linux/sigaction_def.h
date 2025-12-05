#ifndef NOSTR_LINUX_SIGACTION_DEF_H_
#define NOSTR_LINUX_SIGACTION_DEF_H_

#include "../../util/types.h"

typedef union {
  int32_t __sival_int;
  void*   __sival_ptr;
} sigval_t;

struct siginfo_t {
  int32_t si_signo;    /* Signal number                         */
  int32_t si_errno;    /* An errno value                        */
  int32_t si_code;     /* Signal code                           */
  int32_t si_trapno;   /* Trap number that caused               */
                       /* hardware-generated signal             */
                       /* (unused on most architectures)        */
  pid_t    si_pid;     /* Sending process ID                    */
  uid_t    si_uid;     /* Real user ID of sending process       */
  int32_t  si_status;  /* Exit value or signal                  */
  clock_t  si_utime;   /* User time consumed                    */
  clock_t  si_stime;   /* System time consumed                  */
  sigval_t si_value;   /* Signal value                          */
  int32_t  si_int;     /* POSIX.1b signal                       */
  void*    si_ptr;     /* POSIX.1b signal                       */
  int32_t  si_overrun; /* Timer overrun count                   */
                       /* POSIX.1b timers                       */
  int32_t si_timerid;  /* Timer ID; POSIX.1b timers             */
  void*   si_addr;     /* Memory location which caused fault    */
  long    si_band;     /* Band event (was int in                */
                       /* glibc 2.3.2 and earlier)              */
  int32_t si_fd;       /* File descriptor                       */
  int16_t si_addr_lsb; /* Least significant bit of address      */
                       /* (since Linux 2.6.32)                  */
  void* si_lower;      /* Lower bound when address violation    */
                       /* occurred (since Linux 3.19)           */
  void* si_upper;      /* Upper bound when address violation    */
                       /* occurred (since Linux 3.19)           */
  int32_t si_pkey;     /* Protection key on PTE that caused     */
                       /* fault (since Linux 4.6)               */
  void* si_call_addr;  /* Address of system call instruction    */
                       /* (since Linux 3.5)                     */
  int32_t si_syscall;  /* Number of attempted system call       */
                       /* (since Linux 3.5)                     */
  uint32_t si_arch;    /* Architecture of attempted system call */
                       /* (since Linux 3.5)                     */
};

typedef uint64_t         sigset_t;
typedef void             signalfunc_t(int32_t);
typedef void             sigactionfunc_t(int, struct siginfo_t*, void*);
typedef void             restorefunc_t(void);
typedef signalfunc_t*    sighandler_t;
typedef sigactionfunc_t* sigaction_t;
typedef restorefunc_t*   sigrestore_t;

struct sigaction {
#if defined __USE_POSIX199309 || defined __USE_XOPEN_EXTENDED
  union {
    sighandler_t sa_handler;
    sigaction_t  sa_sigaction;
  } __sigaction_handler;
#define sa_handler __sigaction_handler.sa_handler
#define sa_sigaction __sigaction_handler.sa_sigaction
#else
  sighandler_t sa_handler;
#endif
  // glibc
  // sigset_t     sa_mask;
  // uint64_t     sa_flags;
  // sigrestore_t sa_restorer;
  // kernel
  uint64_t     sa_flags;
  sigrestore_t sa_restorer;
  sigset_t     sa_mask;
};

#ifndef SA_RESTORER
#define SA_RESTORER 0x04000000
#endif

#ifndef SIGINT
#define SIGINT 2  // Interactive attention signal.
#endif

#ifndef SIGILL
#define SIGILL 4  // Illegal instruction.
#endif

#ifndef SIGABRT
#define SIGABRT 6  // Abnormal termination.
#endif

#ifndef SIGFPE
#define SIGFPE 8  // Erroneous arithmetic operation.
#endif

#ifndef SIGSEGV
#define SIGSEGV 11  // Invalid access to storage.
#endif

#ifndef SIGTERM
#define SIGTERM 15  // Termination request.
#endif

// Historical signals specified by POSIX.
#ifndef SIGHUP
#define SIGHUP 1  // Hangup.
#endif

#ifndef SIGQUIT
#define SIGQUIT 3  // Quit.
#endif

#ifndef SIGTRAP
#define SIGTRAP 5  // Trace/breakpoint trap.
#endif

#ifndef SIGKILL
#define SIGKILL 9  // Killed.
#endif

#ifndef SIGBUS
#define SIGBUS 10  // Bus error.
#endif

#ifndef SIGSYS
#define SIGSYS 12  // Bad system call.
#endif

#ifndef SIGPIPE
#define SIGPIPE 13  // Broken pipe.
#endif

#ifndef SIGALRM
#define SIGALRM 14  // Alarm clock.
#endif

// New(er) POSIX signals (1003.1-2008, 1003.1-2013).
#ifndef SIGURG
#define SIGURG 16  // Urgent data is available at a socket.
#endif

#ifndef SIGSTOP
#define SIGSTOP 17  // Stop, unblockable.
#endif

#ifndef SIGTSTP
#define SIGTSTP 18  // Keyboard stop.
#endif

#ifndef SIGCONT
#define SIGCONT 19  // Continue.
#endif

#ifndef SIGCHLD
#define SIGCHLD 20  // Child terminated or stopped.
#endif

#ifndef SIGTTIN
#define SIGTTIN 21  // Background read from control terminal.
#endif

#ifndef SIGTTOU
#define SIGTTOU 22  // Background write to control terminal.
#endif

#ifndef SIGPOLL
#define SIGPOLL 23  // Pollable event occurred (System V).
#endif

#ifndef SIGXCPU
#define SIGXCPU 24  // CPU time limit exceeded.
#endif

#ifndef SIGXFSZ
#define SIGXFSZ 25  // File size limit exceeded.
#endif

#ifndef SIGVTALRM
#define SIGVTALRM 26  // Virtual timer expired.
#endif

#ifndef SIGPROF
#define SIGPROF 27  // Profiling timer expired.
#endif

#ifndef SIGUSR1
#define SIGUSR1 30  // User-defined signal 1.
#endif

#ifndef SIGUSR2
#define SIGUSR2 31  // User-defined signal 2.
#endif

// Nonstandard signals found in all modern POSIX systems (including both BSD and Linux).
#ifndef SIGWINCH
#define SIGWINCH 28  // Window size change (4.3 BSD, Sun).
#endif

// Archaic names for compatibility.
#ifndef SIGIO
#define SIGIO SIGPOLL  // I/O now possible (4.2 BSD).
#endif

#ifndef SIGIOT
#define SIGIOT SIGABRT  // IOT instruction, abort() on a PDP-11.
#endif

#ifndef SIGCLD
#define SIGCLD SIGCHLD  // Old System V name
#endif

#endif
