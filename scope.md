
TrimDB Properties
==

This document is a work in progress and lays out areas for discussion in terms of guiding principles, goals, and technical approaches.

Transactional? Yes.

.net core 5? .net standard? Ideally, this works cross platform with no platform bindings.

embeddable? Yes.

Concurrent read & write? Yes.

API:
 - iterators
    - prefix search
 - get
 - set
 - batch
 - seq? badger provides managed sequences with lease and range. 


Persistence and Structures:
  - Main structure is Skip Lists. 
     - skip lists with compression keys in-mem, values in value log?
  - Key compression - delta encoding of keys, does this work well with skiplists?
  - LSM? 
  - nvmem?
  - seperate keys from values?
  - wh log
  - intern ids?


