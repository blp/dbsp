/* This is SPSS syntax for generating a summary table.
/* It also works with GNU PSPP 2.0 or later.

DATA LIST LIST(',') NOTABLE FILE='|grep -vh events all.txt'
   /events (F12)
    partitions (F8)
    size (F8)
    command (A40)
    time (F8.2).
VARIABLE LEVEL time (SCALE) events partitions size command (NOMINAL).

*CTABLES
    /TABLE=events > partitions > size > time BY command.

recode command ('cat'=10)('consume'=15)('consume-threaded'=15)('consume-adapter'=30)('consume-adapter2'=30)('consume-adapter8'=30)('rpk'=11)(else=0) INTO command2.
variable level command2 (nominal).

recode command ('cat'=1)('consume'=1)('consume-threaded'=16)('consume-adapter'=1)('consume-adapter2'=3)('consume-adapter8'=8)('rpk'=1)(else=0) INTO threads.
variable level threads (nominal).
format threads (f8.0).

select if command2 <> 0.
value labels size 0 'n/a'.
value labels command2 10 'cat' 15 'rdkafka-rs' 30 'Feldera adapter' 11 'rpk'.
value labels threads 16 'n'.
variable labels command2 'method' size 'message size'.
CTABLES /TABLE=partitions > size > time BY command2 > threads.
