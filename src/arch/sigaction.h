#ifndef NOSTR_INTERNAL_SIGACTION_H_
#define NOSTR_INTERNAL_SIGACTION_H_

#include "linux/x86_64/sigaction.h"

static inline int32_t internal_sigaction(const int32_t signum, struct sigaction* act, struct sigaction* oldact)
{
  return linux_x8664_sigaction(signum, act, oldact);
}

static inline int32_t internal_sigemptyset(sigset_t* set)
{
  return linux_x8664_sigemptyset(set);
}

#endif
