#ifndef POSTGRES_H
#define POSTGRES_H

#define DatumGetPointer(X) ((Pointer) (X))

typedef char *Pointer;
typedef uintptr_t Datum;

typedef signed char int8;
typedef signed short int16;
typedef signed int int32;
#ifdef __APPLE__
typedef int64_t int64;
#else
typedef long int int64;
#endif

typedef unsigned char uint8;
typedef unsigned short uint16;
typedef unsigned int uint32;
#ifdef __APPLE__
typedef uint64_t uint64;
#else
typedef unsigned long int uint64;
#endif

typedef int32 DateADT;
typedef int64 TimeADT;
typedef int64 Timestamp;
typedef int64 TimestampTz;
typedef int64 TimeOffset;
typedef int32 fsec_t;      /* fractional seconds (in microseconds) */

#ifndef INTERVAL_DEFINED
#define INTERVAL_DEFINED 1
typedef struct
{
  TimeOffset time;  /* all time units other than days, months and years */
  int32 day;        /* days, after time for alignment */
  int32 month;      /* months and years, after time for alignment */
} Interval;
#endif /* INTERVAL_DEFINED */

typedef struct varlena
{
  char vl_len_[4];  /* Do not touch this field directly! */
  char vl_dat[];    /* Data content is here */
} varlena;

typedef varlena text;
typedef struct varlena bytea;

/* The following functions have the same name as external PostgreSQL functions */

extern DateADT date_in(const char *str);
extern char *date_out(DateADT date);
extern int interval_cmp(const Interval *interv1, const Interval *interv2);
extern Interval *interval_in(const char *str, int32 typmod);
extern char *interval_out(const Interval *interv);
extern TimeADT time_in(const char *str, int32 typmod);
extern char *time_out(TimeADT time);
extern Timestamp timestamp_in(const char *str, int32 typmod);
extern char *timestamp_out(Timestamp ts);
extern TimestampTz timestamptz_in(const char *str, int32 typmod);
extern char *timestamptz_out(TimestampTz tstz);

#endif /* POSTGRES_H */