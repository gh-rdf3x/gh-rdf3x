bin/rts/runtime/Runtime.o: rts/runtime/Runtime.cpp  include/rts/runtime/DomainDescription.hpp include/rts/runtime/Runtime.hpp

bin/rts/runtime/Runtime.d: rts/runtime/Runtime.cpp $(wildcard  include/rts/runtime/DomainDescription.hpp include/rts/runtime/Runtime.hpp)

