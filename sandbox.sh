#!/bin/env bash

# export PREFIX=

export JOBS=${N:-$(cat /proc/cpuinfo | awk '/^processor/{print $3}' | wc -l)}
export BUILD_TYPE=${BUILD_TYPE:-Debug}
export LLVM_BUILD_TYPE=${LLVM_BUILD_TYPE:-${BUILD_TYPE}}

echo "Creating Sandbox"
echo "Directory: $PREFIX"
echo "Threads: $JOBS"
echo "Excalibur Build Type: $BUILD_TYPE"
echo "LLVM Build Type: $LLVM_BUILD_TYPE"

if [ -z "$PREFIX" ]
then
	echo "No prefix directory was set."
	exit -1
fi

export BASE=$(realpath ${PREFIX})
if [ -z "$BASE" ]
then
	echo "Could not resolve absolute path of prefix directory"
	exit -1
fi

mkdir -p $BASE

# LLVM11
if [[ ! -e $BASE/llvm11/is_installed ]]; then
	echo "Installing LLVM11"
	mkdir -p $BASE/llvm11/src
	git clone -b release/11.x https://github.com/llvm/llvm-project.git $BASE/llvm11/src  || exit 1
	mkdir -p $BASE/llvm11/install
	mkdir -p $BASE/llvm11/build

	cd $BASE/llvm11/build

	cmake -DCMAKE_INSTALL_PREFIX=$BASE/llvm11/install -DCMAKE_BUILD_TYPE=$LLVM_BUILD_TYPE $BASE/llvm11/src/llvm -DLLVM_TARGETS_TO_BUILD="X86;PowerPC;AArch64" -DLLVM_ENABLE_RTTI=On -DLLVM_BUILD_EXAMPLE=Off -DLLVM_BUILD_BENCHMARKS=Off -DLLVM_BUILD_TESTS=Off -DLLVM_INCLUDE_BENCHMARKS=Off || exit 1
	make -j ${JOBS} || exit 1
	make install || exit 1

	cd $BASE/llvm11/install/bin
	mkdir $BASE/llvm11/is_installed
fi


if [[ ! -e $BASE/capstone/is_installed ]]; then
	echo "Installing capstone"
	mkdir -p $BASE/capstone/src
	git clone https://github.com/aquynh/capstone.git $BASE/capstone/src  || exit 1
	mkdir -p $BASE/capstone/install
	mkdir -p $BASE/capstone/build

	cd $BASE/capstone/build

	cmake -DCAPSTONE_BUILD_STATIC_RUNTIME:BOOL=ON -DCAPSTONE_BUILD_SHARED:BOOL=OFF -DCAPSTONE_X86_ATT_DISABLE:BOOL=ON -DCMAKE_INSTALL_PREFIX=$BASE/capstone/install -DCMAKE_BUILD_TYPE=Release $BASE/capstone/src || exit 1
	make -j ${JOBS} || exit 1
	make install || exit 1

	cd $BASE/capstone/install/bin
	mkdir $BASE/capstone/is_installed
fi


if [[ ! -e $BASE/excalibur/is_installed ]]; then
	echo "Installing Excalibur"
	mkdir -p $BASE/excalibur/src
	git clone git@github.com:t1mm3/db_excalibur.git $BASE/excalibur/src  || exit 1
	mkdir -p $BASE/excalibur/install
	mkdir -p $BASE/excalibur/build

	cd $BASE/excalibur/build

	# -DCMAKE_CXX_CPPCHECK:FILEPATH=$BASE/cppcheck/install/bin/cppcheck
	cmake -DCMAKE_INSTALL_PREFIX=$BASE/excalibur/install -DCMAKE_BUILD_TYPE=$BUILD_TYPE $BASE/excalibur/src -DLLVM_DIR=$BASE/llvm11/install/lib/cmake/llvm -DCAPSTONE_PKGCONF_INCLUDE_DIRS:PATH=$BASE/capstone/install/include -DCAPSTONE_PKGCONF_LIBRARY_DIRS:PATH=$BASE/capstone/install/lib || exit 1
	make -j ${JOBS} || exit 1

	mkdir $BASE/excalibur/is_installed
fi
