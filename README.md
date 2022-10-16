# Excalibur
The (embeddable) adaptive JIT-compiling database engine.

## Setup

Excalibur can be installed using sandbox.sh.

For instance, run ```BUILD_TYPE=<BUILD> N=<CORES> PREFIX=<DIR> sandbox.sh``` to install VOILA and its depedencies in ```<DIR>``` while compilation will use up to ```<CORES>``` threads. The setup will use either a Debug or Release build specified via ```BUILD_TYPE=Debug``` or ```BUILD_TYPE=Release```.

The parameter ```N=<CORES>``` is not required. If not set, the number of processors/cores will be used.

*It can happen that "build_git" for some reason does not compile. In that case, you can compile Excalibur manually from the directory <PREFIX>/excalibur/build*


## Experiments and tests
Tests and experiments can be found in cpp/engine/test. Tests can be run with ```test_engine``` and ```run_experiments.py``` runs the experiments