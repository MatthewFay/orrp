#ifndef ORRP_VERSION_H
#define ORRP_VERSION_H

#define ORRP_VER_MAJOR 0
#define ORRP_VER_MINOR 1
#define ORRP_VER_PATCH 0

#define STR_HELPER(x) #x
#define STR(x) STR_HELPER(x)
#define ORRP_VERSION                                                           \
  STR(ORRP_VER_MAJOR) "." STR(ORRP_VER_MINOR) "." STR(ORRP_VER_PATCH)

#endif