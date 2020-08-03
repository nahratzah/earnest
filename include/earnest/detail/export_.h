#ifndef EARNEST_DETAIL_EXPORT__H
#define EARNEST_DETAIL_EXPORT__H

/*
 * Various macros to control symbol visibility in libraries.
 */
#if defined(WIN32)
# ifdef monsoon_tx_EXPORTS
#   define earnest_export_  __declspec(dllexport)
#   define earnest_local_   /* nothing */
# else
#   define earnest_export_  __declspec(dllimport)
#   define earnest_local_   /* nothing */
# endif
#elif defined(__GNUC__) || defined(__clang__)
# define earnest_export_    __attribute__ ((visibility ("default")))
# define earnest_local_     __attribute__ ((visibility ("hidden")))
#else
# define earnest_export_    /* nothing */
# define earnest_local_     /* nothing */
#endif

#endif /* EARNEST_DETAIL_EXPORT__H */
